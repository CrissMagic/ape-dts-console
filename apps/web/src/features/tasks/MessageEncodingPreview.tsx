import { useState } from 'react';
import type { TaskType } from '@scs/contracts';
import { Copy, Info } from 'lucide-react';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { Tooltip, TooltipContent, TooltipTrigger } from '../../components/ui/tooltip';
import { useOperationNotification } from '../../components/useOperationNotification';
import { mappingSourceLabel, type TaskMappingDraft } from './task-mappings';

interface MessageEncodingPreviewProps {
  readonly title: string;
  readonly taskType: TaskType;
  readonly database: string | undefined;
  readonly mappings: readonly TaskMappingDraft[];
  readonly messageFormat: '' | 'avro' | 'json';
  readonly jsonTemplate: '' | 'standard' | 'cloudcanal';
  readonly withFieldDefs: boolean | null;
}

function messageExample(props: MessageEncodingPreviewProps, mapping: TaskMappingDraft | undefined) {
  const before = { id: 'demo-001' };
  const after = { ...before, name: '示例记录' };
  if (props.taskType === 'KAFKA_KAFKA') return after;

  const database = props.database || 'demo_db';
  const schema = mapping?.sourceSchema.trim() || 'demo_schema';
  const table = mapping?.sourceTable.trim() || 'demo_table';
  const row = { operation: 'update', schema, tb: table, before, after };
  if (props.messageFormat !== 'json') {
    return {
      ...row,
      fields:
        props.withFieldDefs === false
          ? null
          : ['id', 'name'].map((name) => ({ name, column_type: 'text', avro_type: 'String' })),
      extra: { db: database },
    };
  }
  if (props.jsonTemplate !== 'cloudcanal') return row;
  return {
    action: 'UPDATE',
    bid: 0,
    db: database,
    schema,
    table,
    ddl: false,
    entryType: 'ROWDATA',
    execTs: 1_792_112_400_000,
    sendTs: 1_792_112_400_000,
    sql: null,
    pks: ['id'],
    before: [before],
    data: [after],
    dbValType: { id: 'text', name: 'text' },
    jdbcType: { id: 12, name: 12 },
  };
}

export function MessageEncodingPreview(props: MessageEncodingPreviewProps) {
  const [selected, setSelected] = useState(0);
  const [copying, setCopying] = useState(false);
  const { holder, setNotice, setError } = useOperationNotification();
  const index = Math.min(selected, Math.max(0, props.mappings.length - 1));
  const mapping = props.mappings[index];
  const raw = props.taskType === 'KAFKA_KAFKA';
  const avro = !raw && props.messageFormat !== 'json';
  const format = raw
    ? '原始转发'
    : avro
      ? 'Avro · 解码示意'
      : props.jsonTemplate === 'cloudcanal'
        ? 'CloudCanal JSON'
        : '标准 JSON';
  const targets = mapping?.targets.map((target) => target.topic.trim()).filter(Boolean) ?? [];
  const content = JSON.stringify(messageExample(props, mapping), null, 2);

  async function copyMessage() {
    setCopying(true);
    try {
      try {
        if (!navigator.clipboard) throw new Error('Clipboard API unavailable');
        await navigator.clipboard.writeText(content);
      } catch {
        // HTTP 私有化部署可能没有 Clipboard API，使用浏览器原生复制能力。
        const previousFocus = document.activeElement;
        const input = document.createElement('textarea');
        input.value = content;
        input.readOnly = true;
        input.style.cssText = 'position:fixed;opacity:0;pointer-events:none';
        document.body.append(input);
        try {
          input.focus();
          input.select();
          if (!document.execCommand('copy')) throw new Error('Copy rejected');
        } finally {
          input.remove();
          if (previousFocus instanceof HTMLElement) previousFocus.focus();
        }
      }
      setNotice('消息内容已复制');
    } catch {
      setError('复制失败，请选择消息内容后手动复制。');
    } finally {
      setCopying(false);
    }
  }

  return (
    <section
      aria-label={props.title}
      className="message-encoding-preview min-w-0 space-y-3 rounded-lg border p-3"
    >
      {holder}
      <div className="flex flex-wrap items-center gap-2">
        <h4 className="text-sm font-medium">{props.title}</h4>
        <Badge variant="secondary">{format}</Badge>
        <span className="text-xs text-muted-foreground">示例数据</span>
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              type="button"
              aria-label="消息示例说明"
              className="rounded-sm text-muted-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <Info className="size-4" />
            </button>
          </TooltipTrigger>
          <TooltipContent className="max-w-sm">
            示例假设 id
            为文本主键，不读取真实数据。实际字段、旧值、主键和类型元数据由源表与运行制品决定。
            {avro && 'Avro 展示解码后的逻辑结构，不是实际发送的二进制字节或 Avro JSON 编码。'}
            {raw && 'Kafka Raw 保持消息内容，不添加 CDC 包装；原消息也可以是非 JSON 或二进制。'}
          </TooltipContent>
        </Tooltip>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="ml-auto"
          aria-label="复制消息内容"
          disabled={copying}
          onClick={() => void copyMessage()}
        >
          <Copy className="size-3.5" />
          复制
        </Button>
      </div>
      {props.mappings.length > 1 ? (
        <select
          aria-label={`${props.title}来源映射`}
          className="h-9 w-full min-w-0 rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
          value={index}
          onChange={(event) => setSelected(Number(event.target.value))}
        >
          {props.mappings.map((row, rowIndex) => (
            <option key={rowIndex} value={rowIndex}>
              {rowIndex + 1}. {mappingSourceLabel(row, props.taskType) || '未选择来源'}
            </option>
          ))}
        </select>
      ) : mapping ? (
        <p className="break-all text-xs text-muted-foreground">
          来源：{mappingSourceLabel(mapping, props.taskType) || '未选择'}
        </p>
      ) : null}
      <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
        <span>目标 Topic：</span>
        {targets.length > 0 ? (
          targets.map((topic) => (
            <Badge key={topic} variant="outline" className="max-w-full whitespace-normal break-all">
              {topic}
            </Badge>
          ))
        ) : (
          <span>未选择</span>
        )}
      </div>
      <pre
        aria-label={`${props.title}（JSON）`}
        tabIndex={0}
        className="max-h-72 overflow-auto rounded-md bg-muted/50 p-4 font-mono text-xs leading-5 outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <code>{content}</code>
      </pre>
    </section>
  );
}
