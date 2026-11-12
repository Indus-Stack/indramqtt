# Open questions

## DR-03-Q01: Qualification with ACL enabled

Status: open. Owner: CTO.

The vendor qualification runs without ACL. Keyed sends sign each
request with the vendor ACL (`AccessKey`, `Signature`, `SecurityToken`
when set). No run proves a keyed send against a server with ACL on.

Recommendation: keep the no-ACL qualification for the MVP. A later
ticket runs the same write path against a server with ACL enabled and
asserts the exact count. Release disposition: after the MVP.
