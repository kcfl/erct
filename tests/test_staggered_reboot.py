"""Tests for staggered reboot after power loss and ground truth recording."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import time
import pytest

from simulator.agent_runner import SimulatorRunner


def test_staggered_reboot_after_power_loss():
    """Verify staggered reboot distribution and exam clock monotonicity after power loss on C-BPL-02.
    
    Requirements:
    - 40 candidates' first-heartbeat-after-restore times spread over at least 5 s.
    - Each first heartbeat lies within candidate's boot delay plus one interval.
    - remaining_s on that heartbeat equals pre-loss remaining_s minus real elapsed seconds (+/- 3 s).
    - Ground truth file is appended with one complete record for the fault.
    """
    with tempfile.TemporaryDirectory(prefix="erct_test_reboot_", ignore_cleanup_errors=True) as tmpdir:
        tmp_path = Path(tmpdir)
        buf_db = str(tmp_path / "test_buf.db")
        gt_path = str(tmp_path / "ground_truth.jsonl")

        interval_s = 0.5
        runner = SimulatorRunner(
            api_base_url="http://127.0.0.1:9999",  # dummy API URL, buffer won't drain in test
            buffer_db_path=buf_db,
            heartbeat_override_s=interval_s,
            ground_truth_path=gt_path,
        )

        centre_id = "C-BPL-02"
        bpl2_cands = [c for c in runner.candidate_clients if c.centre_id == centre_id]
        assert len(bpl2_cands) == 40

        # Step 0: start sessions
        runner.generate_step(0)

        # Run 2 normal steps to establish pre-loss heartbeats
        for s in range(1, 3):
            time.sleep(interval_s)
            runner.generate_step(s)

        # Record pre-loss remaining_s for each candidate
        pre_loss_state = {}
        for c in bpl2_cands:
            assert c.last_good_heartbeat_ts is not None
            pre_loss_state[c.candidate_id] = {
                "remaining_s": c.remaining_s,
                "last_good_hb_ts": c.last_good_heartbeat_ts,
            }

        # Activate power loss on C-BPL-02 for 3 seconds
        t_loss_start_mono = time.monotonic()
        runner._activate_fault(
            command_id=0,
            centre_id=centre_id,
            fault_type="power_loss",
            duration_s=3,
            params={"source": "grid"},
        )

        # Run steps during power loss (should emit 0 heartbeats for C-BPL-02)
        step = 3
        while (time.monotonic() - t_loss_start_mono) < 3.0:
            time.sleep(interval_s)
            runner.generate_step(step)
            step += 1
            # Verify candidates emit nothing while powered off
            for c in bpl2_cands:
                assert c.is_powered_off is True

        # Check expired faults to trigger power restored and staggered reboot
        runner._check_expired_faults()
        t_restore_mono = time.monotonic()

        # Generation loop during restore window (boot delays are in [2, 20])
        # Loop until all 40 candidates have emitted their first heartbeat
        max_wait_s = 25.0
        while not all(c.first_hb_mono is not None for c in bpl2_cands) and (time.monotonic() - t_restore_mono) < max_wait_s:
            time.sleep(interval_s)
            runner.generate_step(step)
            step += 1

        resumed_cands = [c for c in bpl2_cands if c.first_hb_mono is not None]
        assert len(resumed_cands) == 40, f"Only {len(resumed_cands)}/40 candidates resumed within {max_wait_s}s"

        # 1. Verify delay spread over at least 5 s
        first_times = [c.first_hb_mono for c in bpl2_cands]
        spread = max(first_times) - min(first_times)
        print(f"\n[TEST] Resume delay spread across 40 candidates: {spread:.2f}s (min: {min(first_times) - t_restore_mono:.2f}s, max: {max(first_times) - t_restore_mono:.2f}s)")
        assert spread >= 5.0, f"Expected spread >= 5s, got {spread:.2f}s"

        # 2. Verify each candidate lies within its own delay plus one interval
        for c in bpl2_cands:
            cid = c.candidate_id
            actual_delay = c.first_hb_mono - c.restore_start_mono
            expected_delay = c.boot_delay_s
            # Allow interval_s plus small thread/scheduling buffer
            assert expected_delay <= actual_delay <= expected_delay + interval_s + 0.6, (
                f"Candidate {cid}: delay {actual_delay:.2f}s not in [{expected_delay:.2f}, {expected_delay + interval_s + 0.6:.2f}]"
            )

        # 3. Verify remaining_s on that heartbeat equals pre-loss remaining_s minus real elapsed seconds (tolerance 3 s)
        for c in bpl2_cands:
            cid = c.candidate_id
            actual_remaining = c.remaining_s
            pre_remaining = pre_loss_state[cid]["remaining_s"]

            real_elapsed_s = c.first_hb_mono - t_loss_start_mono
            expected_remaining = pre_remaining - int(round(real_elapsed_s))
            diff = abs(actual_remaining - expected_remaining)
            assert diff <= 3.0, (
                f"Candidate {cid}: remaining_s error {diff}s > 3s "
                f"(actual={actual_remaining}, expected={expected_remaining}, elapsed={real_elapsed_s:.1f}s)"
            )

        # 4. Verify ground truth file output
        gt_file = Path(gt_path)
        assert gt_file.is_file(), "Ground truth file was not created"
        lines = gt_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1, f"Expected 1 ground truth line, found {len(lines)}"

        gt_record = json.loads(lines[0])
        assert gt_record["type"] == "power_loss"
        assert gt_record["centre_id"] == centre_id
        assert len(gt_record["candidates"]) == 40
        for cand_rec in gt_record["candidates"]:
            assert cand_rec["resumed_ts"] is not None
            assert cand_rec["lost_s_true"] > 0
            assert cand_rec["last_good_heartbeat_ts"] is not None
            assert cand_rec["unsaved_answers_at_start"] >= 0
