import { useId, useState, type DragEvent } from 'react';
import { Button, Divider, Dropdown, Select, Tree } from 'antd';
import { FolderOutlined, MoreOutlined, PlusOutlined } from '@ant-design/icons';

export type TaskGroupSummary = { id: string; name: string };

type TaskInGroup = { id: string; name: string; groupId?: string | null | undefined };

export function TaskGroupsPanel({
  groups,
  tasks,
  selectedGroupId,
  onSelect,
  onCreate,
  onAssign,
  onRename,
  onDelete,
  onReorder,
  busy,
}: {
  groups: readonly TaskGroupSummary[];
  tasks: readonly TaskInGroup[];
  selectedGroupId: string | null;
  onSelect: (id: string | null) => void;
  onCreate: () => void;
  onAssign: (taskId: string, groupId: string | null) => void;
  onRename: (group: TaskGroupSummary) => void;
  onDelete: (group: TaskGroupSummary) => void;
  onReorder: (groupIds: string[]) => void;
  busy: boolean;
}) {
  const [dropTarget, setDropTarget] = useState<string | null>(null);
  const nodes = [
    { key: 'all', groupId: 'ALL', name: '全部任务', count: tasks.length },
    ...groups.map((group) => ({
      key: `group:${group.id}`,
      groupId: group.id,
      name: group.name,
      count: tasks.filter((task) => task.groupId === group.id).length,
    })),
  ];

  function accepts(event: DragEvent) {
    return !busy && Array.from(event.dataTransfer.types).includes('application/x-scs-task-id');
  }

  return (
    <aside className="min-w-0 space-y-3" aria-label="任务分组导航">
      <div className="flex items-center justify-between gap-2 px-2">
        <span className="text-sm font-medium">任务分组</span>
        <Button
          type="text"
          size="small"
          icon={<PlusOutlined aria-hidden />}
          aria-label="新增任务组"
          onClick={onCreate}
          disabled={busy}
        >
          新增
        </Button>
      </div>
      <Tree.DirectoryTree
        className="task-group-tree max-h-[60dvh] overflow-auto"
        blockNode
        disabled={busy}
        draggable={{ nodeDraggable: (node) => !busy && node.key !== 'all' }}
        allowDrop={({ dropNode, dropPosition }) =>
          !busy && (dropNode.key === 'all' ? dropPosition === 1 : dropPosition !== 0)
        }
        onDrop={({ dragNode, node, dropPosition, dropToGap }) => {
          if (busy || !dropToGap || dragNode.key === node.key) return;
          const from = groups.find((group) => `group:${group.id}` === dragNode.key);
          const target = groups.find((group) => `group:${group.id}` === node.key);
          if (!from || (!target && node.key !== 'all')) return;
          const ids = groups.filter((group) => group.id !== from.id).map((group) => group.id);
          const targetIndex = target ? ids.indexOf(target.id) : -1;
          const relativePosition = dropPosition - Number(node.pos.split('-').at(-1));
          ids.splice(targetIndex + (relativePosition > 0 ? 1 : 0), 0, from.id);
          if (ids.some((id, index) => id !== groups[index]?.id)) onReorder(ids);
        }}
        selectedKeys={nodes
          .filter((node) => node.groupId === selectedGroupId)
          .map((node) => node.key)}
        onSelect={(keys) => {
          const node = nodes.find((item) => item.key === keys[0]);
          if (node && !busy) onSelect(node.groupId);
        }}
        treeData={nodes.map((node) => ({
          key: node.key,
          isLeaf: true,
          icon: <FolderOutlined aria-hidden />,
          title: (
            <div
              className={`flex min-w-0 items-center justify-between gap-1 rounded px-1 ${dropTarget === node.key && !busy ? 'bg-primary/10 ring-1 ring-primary' : ''}`}
              onDragOver={(event) => {
                if (!accepts(event)) return;
                event.preventDefault();
                event.dataTransfer.dropEffect = 'move';
                setDropTarget(node.key);
              }}
              onDragLeave={(event) => {
                if (!(
                  event.relatedTarget instanceof Node &&
                  event.currentTarget.contains(event.relatedTarget)
                )) {
                  setDropTarget(null);
                }
              }}
              onDrop={(event) => {
                setDropTarget(null);
                if (!accepts(event)) return;
                event.preventDefault();
                event.stopPropagation();
                const taskId = event.dataTransfer.getData('application/x-scs-task-id');
                const task = tasks.find((item) => item.id === taskId);
                const destination = node.groupId === 'ALL' ? null : node.groupId;
                if (task && (task.groupId ?? null) !== destination) onAssign(taskId, destination);
              }}
            >
              <span className="truncate" title={node.name}>
                {node.name}
              </span>
              <span className="ml-auto text-xs text-muted-foreground tabular-nums">
                {node.count}
              </span>
              {node.groupId !== 'ALL' && (
                <span
                  onClick={(event) => event.stopPropagation()}
                  onDoubleClick={(event) => event.stopPropagation()}
                  onDragStart={(event) => event.preventDefault()}
                >
                  <Dropdown
                    trigger={['click']}
                    disabled={busy}
                    menu={{
                      items: [
                        { key: 'rename', label: '重命名' },
                        { key: 'delete', label: '删除分组', danger: true },
                      ],
                      onClick: ({ key, domEvent }) => {
                        domEvent.stopPropagation();
                        const group = groups.find((item) => item.id === node.groupId);
                        if (busy || !group) return;
                        if (key === 'rename') onRename(group);
                        else onDelete(group);
                      },
                    }}
                  >
                    <Button
                      type="text"
                      size="small"
                      icon={<MoreOutlined />}
                      disabled={busy}
                      aria-label={`${node.name} 分组操作`}
                    />
                  </Dropdown>
                </span>
              )}
            </div>
          ),
        }))}
      />
    </aside>
  );
}

export function TaskGroupSelect({
  groups,
  value,
  onChange,
  onCreate,
  disabled,
}: {
  groups: readonly TaskGroupSummary[];
  value: string | null;
  onChange: (value: string | null) => void;
  onCreate: () => void;
  disabled: boolean;
}) {
  const id = useId();
  return (
    <div className="min-w-0 space-y-2">
      <label htmlFor={id} className="text-sm font-medium">
        任务分组
      </label>
      <Select
        id={id}
        className="w-full"
        value={value ?? ''}
        disabled={disabled}
        onChange={(next: string) => onChange(next || null)}
        options={[
          { value: '', label: '不分组' },
          ...groups.map((group) => ({ value: group.id, label: group.name })),
        ]}
        popupRender={(menu) => (
          <>
            {menu}
            <Divider className="!my-1" />
            <Button
              type="text"
              block
              icon={<PlusOutlined aria-hidden />}
              disabled={disabled}
              onClick={onCreate}
            >
              快捷创建任务组
            </Button>
          </>
        )}
      />
    </div>
  );
}
