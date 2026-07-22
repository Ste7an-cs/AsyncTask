# Recent Socket Code Doxygen Comment Design

## Goal

The original goal was to add detailed Doxygen-compatible comments to the C++
code introduced or substantially changed between commits `786f05a` and
`67b71a7` without changing runtime behavior. During follow-up review, the user
separately approved two narrowly scoped behavior-completion changes described
below; the final series therefore is not a purely comment-only diff.

## Scope

The production-code scope is the recently added socket awaitable API and its
supporting lifecycle, error, channel, generator, and scheduler code. Public API
headers receive complete contract comments. Private helpers receive comments
where ownership, thread affinity, cleanup ordering, or error propagation is not
obvious from the implementation.

The socket example and socket-related test additions receive comments for
helper types, test intent, and the demonstrated success and failure paths.
Tests do not receive line-by-line narration.

Generated certificates, qmake project files, user-facing Markdown
documentation, build metadata, and unchanged legacy code remain outside the
implementation scope. This design and its companion plan are updated to record
the approved follow-up history accurately.

## User-Approved Behavior Completion

Follow-up review found two gaps whose fixes the user explicitly approved after
the original comment-only work:

- `CoroUdpSocket::receiveDatagram()` now checks an initial
  `UnconnectedState` before draining datagrams, closes the returned awaitable
  normally, and has a dedicated regression test. This increased the complete
  Qt test count from 56 to 57.
- The socket ping-pong example now performs a 20 ms read timeout before sending
  `ping`, then resumes consumption through the same still-open awaitable to
  demonstrate that `await_for()` timeout does not cancel the stream.

Those behavior additions are part of the retained design history. The final
Doxygen-review correction after baseline `46ee146` remains limited to comments
and these design/plan Markdown files; it does not add further C++ behavior
changes.

## Comment Style

Comments combine Doxygen syntax with Google C++ comment-writing conventions:

- Use `/** ... */` for files, classes, public functions, and substantial private
  helpers.
- Use `///` for short Doxygen comments and `///<` for concise member comments.
- Use `@brief`, `@details`, `@tparam`, `@param`, `@return`, `@note`, and
  `@warning` only when they add contract information.
- Write complete sentences with correct punctuation.
- Describe contracts, ownership, observable results, thread-affinity rules, and
  non-obvious reasoning. Do not merely restate identifiers or implementation
  statements.
- Keep comments adjacent to the declaration or construct they document.
- Use consistent terminology: *source Qt object*, *awaitable*, *consumer*,
  *terminal error*, *socket thread*, and *stream*.

## Public API Contracts

Each public socket wrapper class documents:

- That the wrapper does not own the wrapped Qt object.
- That operations execute immediately on the object's affinity thread or are
  queued to it.
- That returned `std::shared_ptr<Awaitable<T>>` objects strongly survive signal
  callbacks until terminal cleanup.
- Whether an operation emits one value or a stream of values.
- How normal peer closure, explicit closure, Qt errors, and source destruction
  appear to consumers.
- That `await_for` timing out only stops that wait attempt and does not cancel
  the underlying subscription.

Server APIs additionally document ownership of returned raw socket pointers:
the Qt server owns pending and accepted sockets, and consumers must respect Qt
object lifetime and thread affinity.

UDP comments state that one `QNetworkDatagram` is emitted per datagram and that
sender metadata and message boundaries are preserved. SSL comments distinguish
transport errors from TLS verification and handshake errors.

## Internal Contracts

The lifecycle support comments explain that:

- `AwaitableCloseGuard` runs cleanup exactly once, on first close or final
  destruction.
- Cleanup callbacks are moved out from under locks before invocation.
- `SocketConnectionRegistry` safely handles registration racing with cleanup.
- Socket signal callbacks strongly capture the shared awaitable by design.
- `QPointer` guards the independently owned Qt source object.
- Cross-thread actions are queued to the Qt object's affinity thread.
- The first channel close records the terminal error and later closes cannot
  replace it.
- Server polling timers detect `close()` because Qt server classes do not emit a
  dedicated stopped-listening signal.

## Example and Test Comments

The socket example documents the success path, every `Result` check, bounded
wait behavior, timeout semantics, and deterministic refused-connection path.

Socket-related test helpers document resource ownership and cleanup. Each newly
added socket test group receives a concise Doxygen description of the behavior
and invariant under test, including TCP, local sockets, UDP, SSL, source
destruction, explicit close, timeout, and error propagation.

## Verification

Verification covers both the approved behavior-completion history and the final
comment-only review correction:

1. Confirm the final review correction after `46ee146` contains only C++
   comments and the design/plan documents, while retaining the two approved
   earlier behavior changes.
2. Run `git diff --check`.
3. Build the test target and socket example from clean temporary directories.
4. Run the complete Qt test executable.
5. Run the socket example and confirm its successful and refused-connection
   output.
6. Inspect public declarations for missing Doxygen blocks and malformed tags.

## Non-Goals

- No API, ABI, or ownership changes; no timing or error-handling changes beyond
  the approved initial-UDP-state closure and 20 ms timeout-then-resume example.
- No refactoring or formatting-only churn.
- No documentation generation configuration or new Doxygen build target.
- No changes to pre-existing untracked content under `docs/research/`.
