-- Re-admitting a removed member is recorded as 'member.readmit' (a moderator
-- decision, distinct from a first invitation), but 0014's check on
-- conversation_audit.action never listed it. Postgres refused the audit row,
-- the invitation rolled back with it, and the member stayed removed while
-- the moderator was told "database error".
--
-- Applied migrations are immutable, so the constraint is replaced with the
-- same list plus the missing action.
ALTER TABLE conversation_audit
    DROP CONSTRAINT conversation_audit_action_check,
    ADD CONSTRAINT conversation_audit_action_check CHECK (action IN (
        'conversation.create', 'conversation.archive',
        'member.invite', 'member.readmit', 'member.join', 'member.leave',
        'member.remove', 'member.transfer', 'member.recover',
        'message.send', 'message.ack',
        'project.create', 'project.grant', 'project.revoke'));
