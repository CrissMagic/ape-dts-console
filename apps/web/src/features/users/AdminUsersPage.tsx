import { useEffect, useState } from 'react';
import { useOperationNotification } from '../../components/useOperationNotification';
import { Button, Drawer, Dropdown, Form, Input, Modal, Select, Space, Table, Tag } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type {
  ActivationGrantResponse,
  AuthenticatedSessionResponse,
  CreateUserRequest,
  UserRole,
  UserSummary,
} from '@scs/contracts';

import { PlatformApiError, usersApi } from '../../api/platform-api';
import { formatDateTime } from '../../lib/date-time';

type PendingAction =
  | { readonly kind: 'CHANGE_ROLE'; readonly role: UserRole; readonly user: UserSummary }
  | {
      readonly kind: 'CHANGE_STATUS';
      readonly status: 'ACTIVE' | 'DISABLED';
      readonly user: UserSummary;
    }
  | { readonly kind: 'INVITE'; readonly request: CreateUserRequest }
  | { readonly kind: 'RESET_CREDENTIALS'; readonly user: UserSummary }
  | { readonly kind: 'REVOKE_SESSIONS'; readonly user: UserSummary };

function pendingActionDescription(action: PendingAction): string {
  switch (action.kind) {
    case 'INVITE':
      return `将创建 ${action.request.displayName}（${action.request.username}）并生成一次性激活码。`;
    case 'CHANGE_STATUS':
      return `${action.status === 'DISABLED' ? '停用' : '启用'} ${action.user.displayName}（${action.user.username}）。`;
    case 'CHANGE_ROLE':
      return action.role === 'ADMIN'
        ? `将 ${action.user.displayName} 提升为管理员，其现有登录会话将失效。`
        : `将 ${action.user.displayName} 降为开发者，其管理员权限和现有登录会话将失效。`;
    case 'RESET_CREDENTIALS':
      return `重置 ${action.user.displayName} 的凭据，立即使其现有密码和登录会话失效。`;
    case 'REVOKE_SESSIONS':
      return `使 ${action.user.displayName} 的全部当前登录会话失效；该用户需要重新登录。`;
  }
}

function pendingActionButtonLabel(action: PendingAction): string {
  switch (action.kind) {
    case 'INVITE':
      return '确认创建邀请';
    case 'CHANGE_STATUS':
      return action.status === 'DISABLED' ? '确认停用' : '确认启用';
    case 'CHANGE_ROLE':
      return action.role === 'ADMIN' ? '确认提升为管理员' : '确认降为开发者';
    case 'RESET_CREDENTIALS':
      return '确认重置凭据';
    case 'REVOKE_SESSIONS':
      return '确认使登录会话失效';
  }
}

function invalidatesCurrentSession(action: PendingAction, currentUserId: string): boolean {
  if (action.kind === 'INVITE' || action.user.id !== currentUserId) {
    return false;
  }
  switch (action.kind) {
    case 'CHANGE_STATUS':
      return action.status === 'DISABLED';
    case 'CHANGE_ROLE':
      return action.role === 'DEVELOPER';
    case 'RESET_CREDENTIALS':
    case 'REVOKE_SESSIONS':
      return true;
  }
}

export function AdminUsersPage({
  onSessionInvalidated,
  session,
}: {
  onSessionInvalidated: () => void;
  session: AuthenticatedSessionResponse;
}) {
  const [users, setUsers] = useState<readonly UserSummary[]>([]);
  const [grant, setGrant] = useState<ActivationGrantResponse | null>(null);
  const [pendingAction, setPendingAction] = useState<PendingAction | null>(null);
  const [pendingActionConfirmed, setPendingActionConfirmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const { holder, setError } = useOperationNotification();
  const [inviteOpen, setInviteOpen] = useState(false);
  const [inviteDirty, setInviteDirty] = useState(false);
  const [inviteForm] = Form.useForm<CreateUserRequest>();

  async function loadUsers() {
    const result = await usersApi.list();
    setUsers(result.users);
  }

  useEffect(() => {
    void loadUsers().catch((caught: unknown) => {
      if (caught instanceof PlatformApiError && caught.status === 401) {
        onSessionInvalidated();
        return;
      }
      setError(
        caught instanceof PlatformApiError ? `读取用户失败：${caught.code}` : '读取用户失败。',
      );
    });
  }, [onSessionInvalidated, setError]);

  function run(action: () => Promise<void>) {
    setBusy(true);
    setError(null);
    void action()
      .catch((caught: unknown) => {
        if (caught instanceof PlatformApiError && caught.status === 401) {
          onSessionInvalidated();
          return;
        }
        setError(
          caught instanceof PlatformApiError
            ? caught.code === 'FIXED_ADMIN_PROTECTED'
              ? '固定管理员不可降级。'
              : `操作失败：${caught.code}`
            : '操作失败。',
        );
      })
      .finally(() => setBusy(false));
  }

  function invite(request: CreateUserRequest) {
    setPendingAction({
      kind: 'INVITE',
      request,
    });
    setPendingActionConfirmed(false);
    setError(null);
  }

  function requestConfirmation(action: PendingAction) {
    setPendingAction(action);
    setPendingActionConfirmed(false);
    setError(null);
  }

  function confirmPendingAction() {
    if (pendingAction === null || !pendingActionConfirmed) {
      return;
    }
    const currentSessionInvalidated = invalidatesCurrentSession(pendingAction, session.user.id);
    run(async () => {
      switch (pendingAction.kind) {
        case 'INVITE': {
          const created = await usersApi.create(pendingAction.request);
          setGrant(created);
          inviteForm.resetFields();
          setInviteDirty(false);
          setInviteOpen(false);
          break;
        }
        case 'CHANGE_STATUS':
          await usersApi.changeStatus(pendingAction.user, pendingAction.status);
          break;
        case 'CHANGE_ROLE': {
          const changed = await usersApi.changeRole(pendingAction.user, pendingAction.role);
          if ('activationCode' in changed) {
            setGrant(changed);
          }
          break;
        }
        case 'RESET_CREDENTIALS': {
          const reset = await usersApi.resetCredentials(pendingAction.user);
          setGrant(reset);
          break;
        }
        case 'REVOKE_SESSIONS':
          await usersApi.revokeSessions(pendingAction.user);
          break;
      }
      setPendingAction(null);
      setPendingActionConfirmed(false);
      if (currentSessionInvalidated) {
        onSessionInvalidated();
        return;
      }
      await loadUsers();
    });
  }

  function closeInvite() {
    if (!inviteDirty || grant !== null) {
      setInviteOpen(false);
      return;
    }
    Modal.confirm({
      cancelText: '继续编辑',
      content: '尚未提交的邀请信息将丢失。',
      okButtonProps: { danger: true },
      okText: '放弃填写',
      onOk: () => {
        inviteForm.resetFields();
        setInviteDirty(false);
        setInviteOpen(false);
      },
      title: '放弃邀请用户？',
    });
  }

  const columns: ColumnsType<UserSummary> = [
    {
      key: 'user',
      title: '用户',
      render: (_, user) => (
        <Space orientation="vertical" size={0}>
          <Space size="small">
            <strong>{user.displayName}</strong>
            {user.id === session.user.id && <Tag>当前账号</Tag>}
          </Space>
          <span>{user.username}</span>
        </Space>
      ),
    },
    {
      dataIndex: 'role',
      title: '角色',
      render: (role: UserRole) => (role === 'ADMIN' ? '管理员' : '开发者'),
    },
    {
      dataIndex: 'status',
      title: '状态',
      render: (status: UserSummary['status']) =>
        ({
          ACTIVE: '正常',
          DISABLED: '已停用',
          INVITED: '待激活',
          PASSWORD_REQUIRED: '待设置密码',
          TOTP_REQUIRED: '待迁移账号',
        })[status],
    },
    {
      align: 'right',
      key: 'actions',
      title: '操作',
      render: (_, user) => (
        <Dropdown
          trigger={['click']}
          menu={{
            items: [
              ...(user.status === 'ACTIVE' || user.status === 'DISABLED'
                ? [
                    {
                      key: 'status',
                      label: user.status === 'ACTIVE' ? '停用' : '启用',
                      danger: user.status === 'ACTIVE',
                      onClick: () =>
                        requestConfirmation({
                          kind: 'CHANGE_STATUS',
                          status: user.status === 'ACTIVE' ? 'DISABLED' : 'ACTIVE',
                          user,
                        }),
                    },
                  ]
                : []),
              ...(user.status === 'ACTIVE'
                ? [
                    {
                      key: 'role',
                      disabled: user.username === 'admin' && user.role === 'ADMIN',
                      label:
                        user.username === 'admin' && user.role === 'ADMIN'
                          ? '固定管理员不可降级'
                          : user.role === 'ADMIN'
                            ? '降为开发者'
                            : '升为管理员',
                      onClick: () =>
                        requestConfirmation({
                          kind: 'CHANGE_ROLE',
                          role: user.role === 'ADMIN' ? 'DEVELOPER' : 'ADMIN',
                          user,
                        }),
                    },
                  ]
                : []),
              ...(user.status !== 'DISABLED'
                ? [
                    {
                      key: 'reset',
                      label: user.status === 'INVITED' ? '重发邀请' : '重置凭据',
                      danger: true,
                      onClick: () => requestConfirmation({ kind: 'RESET_CREDENTIALS', user }),
                    },
                  ]
                : []),
              ...(user.status === 'ACTIVE'
                ? [
                    {
                      key: 'sessions',
                      label: '使登录会话失效',
                      danger: true,
                      onClick: () => requestConfirmation({ kind: 'REVOKE_SESSIONS', user }),
                    },
                  ]
                : []),
            ],
          }}
        >
          <Button disabled={busy || pendingAction !== null} aria-label={`管理 ${user.username}`}>
            更多操作
          </Button>
        </Dropdown>
      ),
      width: 140,
    },
  ];

  return (
    <section aria-label="用户管理">
      <div className="content-heading justify-end">
        <Space>
          <span className="status-row">{session.user.displayName}</span>
          <Button type="primary" onClick={() => setInviteOpen(true)}>
            邀请用户
          </Button>
        </Space>
      </div>

      {holder}

      <Drawer destroyOnHidden open={inviteOpen} size="large" title="邀请用户" onClose={closeInvite}>
        <Form<CreateUserRequest>
          form={inviteForm}
          initialValues={{ role: 'DEVELOPER' }}
          layout="vertical"
          onFinish={invite}
          onValuesChange={() => setInviteDirty(true)}
        >
          <Form.Item
            label="用户名"
            name="username"
            rules={[{ required: true, message: '请输入用户名' }]}
          >
            <Input autoComplete="off" />
          </Form.Item>
          <Form.Item
            label="显示名称"
            name="displayName"
            rules={[{ required: true, message: '请输入显示名称' }]}
          >
            <Input autoComplete="off" />
          </Form.Item>
          <Form.Item label="角色" name="role" rules={[{ required: true }]}>
            <Select
              options={[
                { label: '开发者', value: 'DEVELOPER' },
                { label: '管理员', value: 'ADMIN' },
              ]}
            />
          </Form.Item>
          <Button block disabled={busy} htmlType="submit" type="primary">
            创建一次性邀请
          </Button>
        </Form>
      </Drawer>

      <Modal
        closable={false}
        footer={
          <Button onClick={() => setGrant(null)} type="primary">
            我已安全保存，关闭
          </Button>
        }
        mask={{ closable: false }}
        open={grant !== null}
        title="一次性激活凭据"
      >
        {grant !== null && (
          <div className="secret-block" role="status">
            <span>一次性激活码</span>
            <code>{grant.activationCode}</code>
            <p>有效期至 {formatDateTime(grant.expiresAt)}</p>
          </div>
        )}
      </Modal>

      {pendingAction !== null && (
        <Modal
          title="确认高风险操作"
          open
          footer={null}
          mask={{ closable: false }}
          closable={!busy}
          keyboard={!busy}
          onCancel={() => {
            if (busy) return;
            setPendingAction(null);
            setPendingActionConfirmed(false);
          }}
        >
          <p>{pendingActionDescription(pendingAction)}</p>
          {pendingAction.kind !== 'INVITE' && pendingAction.user.id === session.user.id && (
            <p role="alert">
              正在操作你当前登录的账号；停用、降级、重置凭据或使会话失效会要求你重新登录。
            </p>
          )}
          <label className="checkbox-label">
            <input
              checked={pendingActionConfirmed}
              onChange={(event) => setPendingActionConfirmed(event.target.checked)}
              type="checkbox"
            />
            我已了解此操作的影响，并确认继续。
          </label>
          <div className="user-actions confirmation-actions">
            <button
              className="secondary-button"
              disabled={busy}
              onClick={() => {
                setPendingAction(null);
                setPendingActionConfirmed(false);
              }}
              type="button"
            >
              取消
            </button>
            <button
              className="primary-button"
              disabled={busy || !pendingActionConfirmed}
              onClick={confirmPendingAction}
              type="button"
            >
              {pendingActionButtonLabel(pendingAction)}
            </button>
          </div>
        </Modal>
      )}

      <section className="bootstrap-panel">
        <Table<UserSummary>
          aria-label="平台用户"
          columns={columns}
          dataSource={[...users]}
          pagination={false}
          rowKey="id"
        />
      </section>
    </section>
  );
}
