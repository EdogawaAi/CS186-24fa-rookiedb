package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

        // 处理 NL 请求
        if (requestType == LockType.NL) {
            return;
        }

        // 首先确保祖先节点有合适的意向锁
        if (requestType == LockType.S) {
            ensureIntentLocks(parentContext, transaction, LockType.IS);
        } else if (requestType == LockType.X) {
            ensureIntentLocks(parentContext, transaction, LockType.IX);
        }

        // 处理当前节点
        if (explicitLockType == LockType.NL) {
            // 没有锁，直接获取
            lockContext.acquire(transaction, requestType);
        } else if (explicitLockType == LockType.IX && requestType == LockType.S) {
            // IX -> SIX 的特殊情况
            List<ResourceName> sisDescendants = new ArrayList<>();
            List<Lock> locks = lockContext.lockman.getLocks(transaction);
            // 添加所有IS和S的后代锁
            for (Lock lock : locks) {
                if (lock.name.isDescendantOf(lockContext.getResourceName()) &&
                        (lock.lockType == LockType.IS || lock.lockType == LockType.S)) {
                    sisDescendants.add(lock.name);
                }
            }
            // 对于要升级的当前锁也要加入释放列表
            sisDescendants.add(lockContext.getResourceName());
            // 执行获取和释放操作
            lockContext.lockman.acquireAndRelease(transaction,
                    lockContext.getResourceName(),
                    LockType.SIX,
                    sisDescendants);
        } else if (explicitLockType.isIntent()) {
            // 意向锁需要升级
            lockContext.escalate(transaction);
            explicitLockType = lockContext.getExplicitLockType(transaction);
            if (!LockType.substitutable(explicitLockType, requestType)) {
                lockContext.promote(transaction, requestType);
            }
        } else if (!LockType.substitutable(explicitLockType, requestType)) {
            // 其他情况需要升级
            lockContext.promote(transaction, requestType);
        }
        return;
    }

    // TODO(proj4_part2) add any helper methods you want
    private static void ensureIntentLocks(LockContext lockContext, TransactionContext transaction, LockType lockType) {
        if (lockContext == null) {
            return;
        }

        // 递归确保父节点的意向锁
        ensureIntentLocks(lockContext.parentContext(), transaction, lockType);

        LockType currentLockType = lockContext.getExplicitLockType(transaction);
        if (currentLockType == LockType.NL) {
            lockContext.acquire(transaction, lockType);
        } else if (currentLockType == LockType.IS && lockType == LockType.IX) {
            lockContext.promote(transaction, LockType.IX);
        } else if (!LockType.substitutable(currentLockType, lockType) &&
                currentLockType != LockType.S && currentLockType != LockType.X &&
                currentLockType != LockType.SIX) {
            lockContext.promote(transaction, lockType);
        }
    }
}
