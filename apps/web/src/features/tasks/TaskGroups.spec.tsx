import type { ComponentProps } from 'react';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { TaskGroupsPanel, TaskGroupSelect } from './TaskGroups';

const groups = [
  { id: 'orders', name: '订单任务' },
  { id: 'finance', name: '财务任务' },
];

function panel(overrides: Partial<ComponentProps<typeof TaskGroupsPanel>> = {}) {
  const props = {
    groups,
    tasks: [
      { id: 'task-1', name: '订单同步', groupId: 'orders' },
      { id: 'task-2', name: '客户同步', groupId: null },
    ],
    selectedGroupId: 'ALL',
    onSelect: vi.fn(),
    onCreate: vi.fn(),
    onAssign: vi.fn(),
    onRename: vi.fn(),
    onDelete: vi.fn(),
    onReorder: vi.fn(),
    busy: false,
    ...overrides,
  };
  render(<TaskGroupsPanel {...props} />);
  return props;
}

function transfer(taskId: string, type = 'application/x-scs-task-id') {
  return { types: [type], getData: vi.fn((key: string) => (key === type ? taskId : '')) };
}

afterEach(cleanup);

describe('task groups', () => {
  it('shows visible task counts and selects real or virtual folders', () => {
    const props = panel();
    expect(screen.getByRole('treeitem', { name: /全部任务.*2/ })).toBeInTheDocument();
    expect(screen.getByRole('treeitem', { name: /订单任务.*1/ })).toBeInTheDocument();
    expect(screen.getByRole('treeitem', { name: /财务任务.*0/ })).toBeInTheDocument();
    fireEvent.click(screen.getByText('订单任务'));
    expect(props.onSelect).toHaveBeenLastCalledWith('orders');
    expect(screen.queryByText('未分组')).not.toBeInTheDocument();
    fireEvent.click(screen.getByText('全部任务'));
    expect(props.onSelect).toHaveBeenLastCalledWith('ALL');
    fireEvent.click(screen.getByRole('button', { name: '新增任务组' }));
    expect(props.onCreate).toHaveBeenCalledOnce();
  });

  it('moves visible tasks to folders and clears grouping by dropping onto All', () => {
    const props = panel();
    fireEvent.drop(screen.getByText('财务任务'), { dataTransfer: transfer('task-1') });
    expect(props.onAssign).toHaveBeenLastCalledWith('task-1', 'finance');
    fireEvent.drop(screen.getByText('全部任务'), { dataTransfer: transfer('task-1') });
    expect(props.onAssign).toHaveBeenLastCalledWith('task-1', null);
    fireEvent.drop(screen.getByText('订单任务'), { dataTransfer: transfer('unknown-task') });
    fireEvent.drop(screen.getByText('财务任务'), {
      dataTransfer: transfer('task-1', 'text/plain'),
    });
    fireEvent.drop(screen.getByText('订单任务'), { dataTransfer: transfer('task-1') });
    expect(props.onAssign).toHaveBeenCalledTimes(2);
  });

  it('does not allow mutations or selection while busy', () => {
    const props = panel({ busy: true });
    expect(screen.getByRole('button', { name: '新增任务组' })).toBeDisabled();
    fireEvent.click(screen.getByText('订单任务'));
    fireEvent.drop(screen.getByText('财务任务'), { dataTransfer: transfer('task-1') });
    expect(props.onSelect).not.toHaveBeenCalled();
    expect(props.onAssign).not.toHaveBeenCalled();
  });

  it('uses the existing tree drag interaction to reorder folders while keeping All fixed', () => {
    const props = panel();
    // JSDOM has no layout or DragEvent; provide pointer coordinates for the real Tree handler.
    const drag = (element: Element, type: string) => {
      const event = new Event(type, { bubbles: true, cancelable: true });
      Object.defineProperties(event, {
        clientX: { value: 0 },
        clientY: { value: 0 },
        dataTransfer: { value: { types: [], setData: vi.fn(), getData: () => '' } },
      });
      fireEvent(element, event);
    };
    const orders = screen.getByText('订单任务').closest('.ant-tree-node-content-wrapper')!;
    const finance = screen.getByText('财务任务').closest('.ant-tree-node-content-wrapper')!;
    drag(orders, 'dragstart');
    drag(finance, 'dragenter');
    drag(finance, 'dragover');
    drag(finance, 'drop');
    expect(props.onReorder).toHaveBeenLastCalledWith(['finance', 'orders']);
    drag(finance, 'dragstart');
    vi.spyOn(orders, 'getBoundingClientRect').mockReturnValue({ top: 0, height: 24 } as DOMRect);
    drag(orders, 'dragenter');
    drag(orders, 'dragover');
    drag(orders, 'drop');
    expect(props.onReorder).toHaveBeenCalledTimes(2);
    expect(props.onReorder).toHaveBeenLastCalledWith(['finance', 'orders']);
    expect(props.onAssign).not.toHaveBeenCalled();
  });

  it('offers rename and delete from an existing folder menu without changing the selected folder', async () => {
    const props = panel();
    fireEvent.click(screen.getByRole('button', { name: '订单任务 分组操作' }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '重命名' }));
    expect(props.onRename).toHaveBeenCalledWith(groups[0]);
    expect(props.onSelect).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '订单任务 分组操作' }));
    fireEvent.click(await screen.findByRole('menuitem', { name: '删除分组' }));
    expect(props.onDelete).toHaveBeenCalledWith(groups[0]);
    expect(props.onSelect).not.toHaveBeenCalled();
  });

  it('selects a group or ungrouped in the editor and offers quick creation', () => {
    const onChange = vi.fn();
    const onCreate = vi.fn();
    const { rerender } = render(
      <TaskGroupSelect
        groups={groups}
        value={null}
        onChange={onChange}
        onCreate={onCreate}
        disabled={false}
      />,
    );
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '任务分组' }));
    fireEvent.click(screen.getByText('订单任务'));
    expect(onChange).toHaveBeenLastCalledWith('orders');
    rerender(
      <TaskGroupSelect
        groups={groups}
        value="orders"
        onChange={onChange}
        onCreate={onCreate}
        disabled={false}
      />,
    );
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '任务分组' }));
    fireEvent.click(screen.getByText('不分组'));
    expect(onChange).toHaveBeenLastCalledWith(null);
    fireEvent.mouseDown(screen.getByRole('combobox', { name: '任务分组' }));
    fireEvent.click(screen.getByRole('button', { name: '快捷创建任务组' }));
    expect(onCreate).toHaveBeenCalledOnce();
  });

  it('disables group selection while the editor is busy', () => {
    render(
      <TaskGroupSelect
        groups={groups}
        value={null}
        onChange={vi.fn()}
        onCreate={vi.fn()}
        disabled
      />,
    );
    expect(screen.getByRole('combobox', { name: '任务分组' })).toBeDisabled();
  });
});
