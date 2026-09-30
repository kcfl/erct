# PROMPT 9b-2: FAILING TESTS AND A FALSE REPORT

Read this whole file. First reply with a numbered list of ALL step titles you found,
then continue immediately with STEP 0 without waiting for me.

GROUND RULES
 R1. Every explanation must be backed by a command whose raw output you paste. Otherwise
     write "unverified".
 R2. Test output: run `pytest -v 2>&1 | Tee-Object -FilePath data/runs/pytest_<UTC timestamp>.txt`
     and paste that file unedited. Never retype or reformat test output.
 R3. Never kill processes by name or in bulk. Keep the PID of every process you start.
 R4. Never rewrite a test so that it passes and never loosen an assertion. Fix the code
     or the test infrastructure. If a test's intent must change, say so under
     "Deviations" with the reason.
 R5. Do not commit or tag before the full suite has finished and is green.
 R6. I run `pytest -v` myself after your reply. Any mismatch between your pasted output
     and mine means the whole report is rejected.

CONTEXT
I ran the full suite myself: 74 collected, 72 passed, 2 FAILED:
  test_13_slow_early_fault_network[0.5]: manual_review 22/40 = 55.0%, strong=18
  test_13_slow_early_fault_network[2.0]: manual_review 40/40 = 100.0%, strong=0
Your report said "74 passed" and "Not done: none". That report was false, and the tag
phase-3b-iii-b sits on failing code. Also, your isolated runs of test_13 passed, and your
3b-ii report already mentioned "intermittent false-positive manual review during
full-suite execution", which you patched with runner.buffer.drain_all() in the test. My
hypothesis: the results depend on test order, through state leaking between tests
(threads, servers, config, environment). This is a hypothesis, not a fact.
Also: my git history was rewritten with filter-branch (a document was removed), so commit
hashes and tags changed. Do not be confused by that.

STEP 0: THE FALSE REPORT
 In 5 lines: did you see the final result of your last `pytest -v` before you wrote
 "74 passed", and did you wait for it to finish? Answer from your own command log. Then
 list every item from prompt 9b that you reported as done without evidence.

STEP 1: REPRODUCE AND LOCALIZE (paste the raw results of each)
 a. test_13 alone, both variants:
    pytest -v -s "tests/test_decisions_fairness_notices.py::test_13_slow_early_fault_network" --basetemp=data/runs/t13_alone
 b. The whole decisions file alone:
    pytest -v tests/test_decisions_fairness_notices.py --basetemp=data/runs/t13_file
 c. test_impact + decisions together:
    pytest -v tests/test_impact.py tests/test_decisions_fairness_notices.py
 d. The full suite per R2.
 State which combinations fail. If a test passes alone but fails after other tests, that
 is order dependence and the search below is mandatory.

STEP 2: EVIDENCE FROM A FAILING RUN
 Keep the database of a failing test_13 (via --basetemp) and print:
  - the incident row (window_start, window_end, status, resolved_at, impact_computed_at);
  - impact rows grouped by evidence_quality and by reason (from quality_details);
  - for 3 candidates: the HEARTBEAT events between window_start - 12 s and window_end + 6 s
    with ts AND ingested_at;
  - the number of 'impact' audit entries per candidate (recompute history);
  - impact_computed_at versus the maximum ingested_at of events whose ts lies inside the
    window (was impact computed before the backlog arrived?).
 Then run the same scenario through demo/live_fault.py
  (--fault network_drop:C-BPL-04:30:10 --interval 2.0) and list every difference between the
 test setup and the demo setup (config values, heartbeat override, boot delay, seed,
 timing of the fault, control key, drain_all, threads).

STEP 3: STATE LEAKING BETWEEN TESTS
 List every place that keeps process-wide state: the get_config cache and reload_config,
 os.environ ERCT_CONFIG_PATH, database path globals, DetectionWorker and other background
 threads, the sender thread, uvicorn servers started in threads, DB_WRITE_LOCK, module
 level engines. For test_09, test_10, test_11, test_12, test_13 and the detection tests say
 whether each stops its server, its detection worker and its sender, and restores config and
 environment, in a finally block or a fixture.
 Add tests/conftest.py with an autouse fixture that: records threading.enumerate() before
 and after every test, fails the test if an app thread (detection worker, sender, uvicorn)
 survives its teardown, and restores config and environment on teardown.

STEP 4: FIX THE CAUSE
 Fix the leak or the bug in code or fixtures. Do not touch the assertions. Remove
 drain_all() from test_13 unless you can prove it is needed for a legitimate reason; if you
 keep it, explain what a real deployment does instead, because production has no drain_all.
 If the cause is in the engine (impact computed before the backlog arrived, recompute not
 triggered), fix it there and add a test that fails without the fix.

STEP 5: PROVE IT
 Run the full suite per R2 TWICE in a row. Both runs must show 74 passed. Paste both files
 unedited. Also run test_13 alone once more. Then:
  git add . ; git commit -m "phase-3b-iii-b2" ; git tag phase-3b-iii-b2
 Do not move or delete the existing tags.

STEP 6: HYGIENE
 Paste `git log --oneline --decorate -5`, `git status`, and `git ls-files`. Confirm that
 scratch/, data/ and *.db are not tracked and that docs/mponlineideathon.pdf is not tracked.
 Add missing entries to .gitignore.

VERIFICATION
 1. The pytest files per R2, unedited.
 2. A STEP CHECKLIST first in your reply: one line per numbered step and sub-step, DONE /
    PARTIAL / NOT DONE with the evidence (command or file). "Not done: none" is only
    allowed if every line is DONE with evidence.
 3. Sections "Deviations" and "Questions".
