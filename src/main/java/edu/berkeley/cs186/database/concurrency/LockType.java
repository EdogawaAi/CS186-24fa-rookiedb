package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        /**
         * Compatibility Matrix
         * (Boolean value in cell answers is `left` compatible with `top`?)
         *
         *     | NL  | IS  | IX  |  S  | SIX |  X
         * ----+-----+-----+-----+-----+-----+-----
         * NL  |  T  |  T  |  T  |  T  |  T  |  T
         * ----+-----+-----+-----+-----+-----+-----
         * IS  |  T  |  T  |  T  |  T  |     |
         * ----+-----+-----+-----+-----+-----+-----
         * IX  |  T  |  T  |  T  |  F  |     |
         * ----+-----+-----+-----+-----+-----+-----
         * S   |  T  |  T  |  F  |  T  |  F  |  F
         * ----+-----+-----+-----+-----+-----+-----
         * SIX |  T  |     |     |  F  |     |
         * ----+-----+-----+-----+-----+-----+-----
         * X   |  T  |     |     |  F  |     |  F
         * ----+-----+-----+-----+-----+-----+-----
         *
         * The filled in cells are covered by the public tests.
         * You can expect the blank cells to be covered by the hidden tests!
         * Hint: I bet the notes might have something useful for this...
         */
        // TODO(proj4_part1): implement
        // S, X, IS, IX, SIX, NL
        boolean[][] compatibilityMatrix = {
                {true, false, true, false, false, true},
                {false, false, false, false, false, true},
                {true, false, true, true, false, true},
                {false, false, true, true, false, true},
                {false, false, false, false, false, true},
                {true, true, true, true, true, true}
        };

        return compatibilityMatrix[a.ordinal()][b.ordinal()];
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        /**
         * Parent Matrix
         * (Boolean value in cell answers can `left` be the parent of `top`?)
         *
         *     | NL  | IS  | IX  |  S  | SIX |  X
         * ----+-----+-----+-----+-----+-----+-----
         * NL  |  T  |  F  |  F  |  F  |  F  |  F
         * ----+-----+-----+-----+-----+-----+-----
         * IS  |  T  |  T  |  F  |  T  |  F  |  F
         * ----+-----+-----+-----+-----+-----+-----
         * IX  |  T  |  T  |  T  |  T  |  T  |  T
         * ----+-----+-----+-----+-----+-----+-----
         * S   |  T  |     |     |     |     |
         * ----+-----+-----+-----+-----+-----+-----
         * SIX |  T  |     |     |     |     |
         * ----+-----+-----+-----+-----+-----+-----
         * X   |  T  |     |     |     |     |
         * ----+-----+-----+-----+-----+-----+-----
         *
         * The filled in cells are covered by the public test.
         * You can expect the blank cells to be covered by the hidden tests!
         */
        // TODO(proj4_part1): implement
        boolean[][] parentMatrix = {
                // S, X, IS, IX, SIX, NL
                {false, false, false, false, false, true},
                {false, false, false, false, false, true},
                {true, false, true, false, false, true},
                {true, true, true, true, true, true},
                {false, false, false, false, false, true},
                {false, false, false, false, false, true}
        };

        return parentMatrix[parentLockType.ordinal()][childLockType.ordinal()];
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        /**
         * Substitutability Matrix
         * (Values along left are `substitute`, values along top are `required`)
         *
         *     | NL  | IS  | IX  |  S  | SIX |  X
         * ----+-----+-----+-----+-----+-----+-----
         * NL  |  T  |  F  |  F  |  F  |  F  |  F
         * ----+-----+-----+-----+-----+-----+-----
         * IS  |     |  T  |  F  |  F  |     |  F
         * ----+-----+-----+-----+-----+-----+-----
         * IX  |     |  T  |  T  |  F  |     |  F
         * ----+-----+-----+-----+-----+-----+-----
         * S   |     |     |     |  T  |     |  F
         * ----+-----+-----+-----+-----+-----+-----
         * SIX |     |     |     |  T  |     |  F
         * ----+-----+-----+-----+-----+-----+-----
         * X   |     |     |     |  T  |     |  T
         * ----+-----+-----+-----+-----+-----+-----
         *
         * The filled in cells are covered by the public test.
         * You can expect the blank cells to be covered by the hidden tests!
         *
         * The boolean value in the cell answers the question:
         * "Can `left` substitute `top`?"
         *
         * or alternatively:
         * "Are the privileges of `left` a superset of those of `top`?"
         */
        // TODO(proj4_part1): implement
        boolean[][] substitutabilityMatrix = {
                // S, X, IS, IX, SIX, NL
                {true, false, false, false, false, false},
                {true, true, false, false, false, false},
                {false, false, true, false, false, false},
                {false, false, true, true, false, false},
                {true, false, false, false, false, false},
                {false, false, false, false, false, true}
        };

        return substitutabilityMatrix[substitute.ordinal()][required.ordinal()];
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

