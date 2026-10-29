import type { ComponentProps } from 'react';
import { cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { MessageEncodingPreview } from './MessageEncodingPreview';
import { emptyTaskMapping, newMappingTarget } from './task-mappings';

const props: ComponentProps<typeof MessageEncodingPreview> = {
  title: '预计消息内容',
  taskType: 'PG_KAFKA',
  database: 'data_demo',
  messageFormat: '',
  jsonTemplate: '',
  withFieldDefs: null,
  mappings: [
    {
      ...emptyTaskMapping(),
      sourceSchema: 'public',
      sourceTable: 'orders',
      targets: [newMappingTarget('orders.a'), newMappingTarget('orders.b')],
    },
    {
      ...emptyTaskMapping(),
      sourceSchema: 'sales',
      sourceTable: 'customers',
      targets: [newMappingTarget('customers')],
    },
  ],
};

function content() {
  return screen.getByLabelText('预计消息内容（JSON）').textContent!;
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  Reflect.deleteProperty(document, 'execCommand');
});

describe('message encoding examples', () => {
  it('copies the formatted content of the currently selected mapping', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal('navigator', { clipboard: { writeText } });
    render(<MessageEncodingPreview {...props} messageFormat="json" jsonTemplate="cloudcanal" />);
    fireEvent.change(screen.getByLabelText('预计消息内容来源映射'), { target: { value: '1' } });
    const expected = content();
    fireEvent.click(screen.getByRole('button', { name: '复制消息内容' }));
    expect(writeText).toHaveBeenCalledWith(expected);
    expect(JSON.parse(expected)).toMatchObject({ schema: 'sales', table: 'customers' });
    expect(await screen.findByText('消息内容已复制')).toBeInTheDocument();
  });

  it('copies on HTTP deployments without the Clipboard API and removes the temporary input', async () => {
    vi.stubGlobal('navigator', {});
    let copied = '';
    const execCommand = vi.fn(() => {
      copied = (document.activeElement as HTMLTextAreaElement).value;
      return true;
    });
    Object.defineProperty(document, 'execCommand', { configurable: true, value: execCommand });
    render(<MessageEncodingPreview {...props} />);
    const expected = content();
    fireEvent.click(screen.getByRole('button', { name: '复制消息内容' }));
    expect(copied).toBe(expected);
    expect(execCommand).toHaveBeenCalledWith('copy');
    expect(await screen.findByText('消息内容已复制')).toBeInTheDocument();
    expect(document.querySelector('textarea')).toBeNull();
  });

  it('reports clipboard rejection instead of claiming success', async () => {
    vi.stubGlobal('navigator', {
      clipboard: { writeText: vi.fn().mockRejectedValue(new Error('denied')) },
    });
    Object.defineProperty(document, 'execCommand', {
      configurable: true,
      value: vi.fn(() => false),
    });
    render(<MessageEncodingPreview {...props} />);
    fireEvent.click(screen.getByRole('button', { name: '复制消息内容' }));
    expect(await screen.findByText('复制失败，请选择消息内容后手动复制。')).toBeInTheDocument();
    expect(screen.queryByText('消息内容已复制')).not.toBeInTheDocument();
    expect(document.querySelector('textarea')).toBeNull();
  });

  it('uses the MySQL database and mapped table for formatted CloudCanal JSON', () => {
    render(
      <MessageEncodingPreview
        {...props}
        taskType="MYSQL_KAFKA"
        database="data_demo"
        mappings={[{ ...props.mappings[0]!, sourceSchema: 'data_demo' }]}
        messageFormat="json"
        jsonTemplate="cloudcanal"
      />,
    );
    expect(JSON.parse(content())).toMatchObject({
      db: 'data_demo',
      schema: 'data_demo',
      table: 'orders',
      action: 'UPDATE',
    });
    expect(screen.getByText('来源：data_demo.orders')).toBeInTheDocument();
    expect(content()).toBe(JSON.stringify(JSON.parse(content()), null, 2));
  });
  it.each(['', 'avro'] as const)(
    'labels %s Avro as a decoded example and honors field definitions',
    (messageFormat) => {
      const view = render(<MessageEncodingPreview {...props} messageFormat={messageFormat} />);
      expect(screen.getByText('Avro · 解码示意')).toBeInTheDocument();
      expect(JSON.parse(content())).toMatchObject({
        operation: 'update',
        schema: 'public',
        tb: 'orders',
        fields: [
          { name: 'id', column_type: 'text', avro_type: 'String' },
          { name: 'name', column_type: 'text', avro_type: 'String' },
        ],
        extra: { db: 'data_demo' },
      });
      view.rerender(
        <MessageEncodingPreview {...props} messageFormat={messageFormat} withFieldDefs={false} />,
      );
      expect(JSON.parse(content()).fields).toBeNull();
    },
  );

  it.each(['', 'standard'] as const)(
    'uses the standard JSON envelope for template %s and pretty prints it',
    (jsonTemplate) => {
      render(
        <MessageEncodingPreview {...props} messageFormat="json" jsonTemplate={jsonTemplate} />,
      );
      const message = JSON.parse(content());
      expect(message).toEqual({
        operation: 'update',
        schema: 'public',
        tb: 'orders',
        before: { id: 'demo-001' },
        after: { id: 'demo-001', name: '示例记录' },
      });
      expect(content()).toBe(JSON.stringify(message, null, 2));
    },
  );

  it('keeps CloudCanal metadata independent of the Avro field definition setting', () => {
    const view = render(
      <MessageEncodingPreview {...props} messageFormat="json" jsonTemplate="cloudcanal" />,
    );
    const original = content();
    expect(JSON.parse(original)).toMatchObject({
      action: 'UPDATE',
      db: 'data_demo',
      table: 'orders',
      pks: ['id'],
      before: [{ id: 'demo-001' }],
      data: [{ id: 'demo-001', name: '示例记录' }],
      dbValType: { id: 'text', name: 'text' },
      jdbcType: { id: 12, name: 12 },
    });
    view.rerender(
      <MessageEncodingPreview
        {...props}
        messageFormat="json"
        jsonTemplate="cloudcanal"
        withFieldDefs={false}
      />,
    );
    expect(content()).toBe(original);
  });

  it('previews the selected mapping and all fanout targets without modifying mappings', () => {
    render(<MessageEncodingPreview {...props} messageFormat="json" jsonTemplate="cloudcanal" />);
    const region = within(screen.getByRole('region', { name: '预计消息内容' }));
    expect(region.getByText('orders.a')).toBeInTheDocument();
    expect(region.getByText('orders.b')).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('预计消息内容来源映射'), { target: { value: '1' } });
    expect(JSON.parse(content())).toMatchObject({ schema: 'sales', table: 'customers' });
    expect(region.queryByText('orders.a')).not.toBeInTheDocument();
    expect(region.getByText('customers', { selector: '[data-slot="badge"]' })).toBeInTheDocument();
    expect(props.mappings[0]!.sourceTable).toBe('orders');
  });

  it('keeps a Kafka Raw example free of CDC wrappers regardless of stale encoding selections', () => {
    render(
      <MessageEncodingPreview
        {...props}
        taskType="KAFKA_KAFKA"
        messageFormat="json"
        jsonTemplate="cloudcanal"
      />,
    );
    expect(JSON.parse(content())).toEqual({ id: 'demo-001', name: '示例记录' });
    expect(screen.getByText('原始转发')).toBeInTheDocument();
  });

  it('supports an incomplete draft and a removed selected mapping without changing configuration', () => {
    const view = render(<MessageEncodingPreview {...props} />);
    fireEvent.change(screen.getByLabelText('预计消息内容来源映射'), { target: { value: '1' } });
    view.rerender(<MessageEncodingPreview {...props} mappings={[]} database={undefined} />);
    expect(JSON.parse(content())).toMatchObject({
      schema: 'demo_schema',
      tb: 'demo_table',
      extra: { db: 'demo_db' },
    });
    expect(screen.getByText('未选择')).toBeInTheDocument();
  });
});
