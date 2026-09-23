package cleanup

import (
	"errors"
	"testing"
)

// capability_id: rainbond.cleanup.single-deletion-attempt
func TestDeletionAttemptIsConsumedOnceAndRemainsProtectedUntilVerified(t *testing.T) {
	database, _ := coordinationDB(t)
	r := operation("selected", "delete", "app/a")
	if _, err := AcquireOperation(database, r); err != nil {
		t.Fatal(err)
	}
	if err := BeginDeletionAttempt(database, r); err != nil {
		t.Fatal(err)
	}
	if err := BeginDeletionAttempt(database, r); !errors.Is(err, ErrCoordinationUncertain) {
		t.Fatal("DELETE attempt granted twice", err)
	}
	if err := FinishOperation(database, r, true); !errors.Is(err, ErrCoordinationUncertain) {
		t.Fatal("in-flight DELETE released", err)
	}
	if err := CompleteDeletionAttempt(database, r, "applied"); err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireOperation(database, operation("writer", "producer", "app/a")); !errors.Is(err, ErrCoordinationBusy) {
		t.Fatal("post-delete verification scope released early", err)
	}
	if err := FinishOperation(database, r, true); err != nil {
		t.Fatal(err)
	}
	if err := BeginDeletionAttempt(database, r); !errors.Is(err, ErrCoordinationChanged) {
		t.Fatal("completed DELETE replayed", err)
	}
}
func TestUnknownDeletionAttemptCannotBecomeSuccessByRetry(t *testing.T) {
	database, _ := coordinationDB(t)
	r := operation("selected", "delete", "app/a")
	if _, err := AcquireOperation(database, r); err != nil {
		t.Fatal(err)
	}
	if err := BeginDeletionAttempt(database, r); err != nil {
		t.Fatal(err)
	}
	if err := CompleteDeletionAttempt(database, r, "unknown"); err != nil {
		t.Fatal(err)
	}
	if err := CompleteDeletionAttempt(database, r, "applied"); !errors.Is(err, ErrCoordinationUncertain) {
		t.Fatal("unknown result overwritten", err)
	}
	if err := FinishOperation(database, r, true); !errors.Is(err, ErrCoordinationUncertain) {
		t.Fatal("unknown result released", err)
	}
}
