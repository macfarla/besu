# Test Run Summary

## Overview
This document summarizes the findings from the test runs conducted to investigate the flaky tests mentioned in the issue [#6909](https://github.com/hyperledger/besu/issues/6909). The focus was on identifying the root cause of the flaky tests and documenting potential solutions.

## Test Details
- **Test Class:** `org.hyperledger.besu.tests.acceptance.pubsub.NewPendingTransactionAcceptanceTest`
- **Gradle Target:** `acceptanceTestMainnet`
- **Repository URL:** [https://github.com/macfarla/besu](https://github.com/macfarla/besu)
- **Branch:** `fix-flaky-tests`

## Test Methods
The following test methods were investigated:
1. `subscriptionToMinerNodeMustReceiveEveryPublishEvent`
2. `everySubscriptionMustReceiveEveryPublishEvent`
3. `subscriptionToArchiveNodeMustReceivePublishEvent`
4. `everySubscriptionMustReceivePublishEvent`
5. `subscriptionToArchiveNodeMustReceiveEveryPublishEvent`

## Findings
- The tests involve asynchronous operations and network interactions, which could be potential sources of flakiness.
- No instances of the words "FAILED" or "SKIPPED" were found in the test run files, indicating that the tests did not fail or get skipped during the runs that were checked.
- The search results did return instances of the word "Error" in various other files within the repository, but these do not seem to be related to the test runs themselves and are likely part of the normal logging or error handling within the codebase.

## Conclusion
The tests ran successfully in the observed runs, and no flakiness was detected during these runs. However, given the nature of flaky tests, it is possible that the flakiness did not manifest in these particular runs. Further monitoring and additional test runs may be required to confirm the stability of the tests.

## Recommendations
- Continue to monitor the test runs for any signs of flakiness.
- Investigate potential sources of flakiness, such as asynchronous operations, network dependencies, state dependency, concurrency, and resource cleanup.
- Consider implementing additional logging and diagnostic measures to capture more detailed information during test execution.

## Attachments
- Detailed test run outputs are saved locally for reference.
  - `~/full_outputs/_gradlew_acceptanceT_1716399809.9716759.txt`
  - `~/full_outputs/search_1716400998.2206345.txt`
