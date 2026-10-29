import { useOperationNotification } from '../../components/useOperationNotification';
import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';
import type {
  AuthenticatedSessionResponse,
  ArtifactSummary,
  DataSourceSummary,
  RuntimeOperationSummary,
  TaskPreflightCheckKey,
  TaskPreflightRunSummary,
  TaskSummary,
  TaskType,
} from '@scs/contracts';
import { pgTableAddition } from '@scs/config-model/task-schema';
import { Checkbox, Pagination, Select } from 'antd';
import {
  ArrowLeft,
  ArrowRight,
  CircleCheck,
  Database,
  Copy,
  Info,
  Loader2,
  MoreHorizontal,
  Plus,
  Play,
  RefreshCw,
  Save,
  ShieldCheck,
  Square,
  Trash2,
} from 'lucide-react';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Badge } from '../../components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '../../components/ui/dialog';

import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '../../components/ui/tooltip';

import {
  dataSourcesApi,
  listArtifacts,
  PlatformApiError,
  tasksApi,
  taskGroupsApi,
  recoveryApi,
} from '../../api/platform-api';
import { type LeaveGuard, useDraftGuard } from '../../components/useDraftGuard';
import { operationName } from '../../lib/operation-labels';
import { TaskRuntimeDetails } from './runtime/TaskRuntimeDetails';
import { TaskCard } from './TaskCard';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
} from '../../components/ui/dropdown-menu';
import './task-cards.css';
import { useTaskProgress } from './progress/useTaskProgress';
import { SourceSinkMappings } from './SourceSinkMappings';
import { MessageEncodingPreview } from './MessageEncodingPreview';
import { TaskGroupsPanel, TaskGroupSelect, type TaskGroupSummary } from './TaskGroups';
import {
  emptyTaskMapping,
  mappingSourceLabel,
  mappingTargets,
  readTaskMappings,
  serializeTaskMappings,
  type TaskMappingDraft,
  type TaskMappingTarget,
} from './task-mappings';

type ResourceMode = 'SELF_MANAGED' | 'PLATFORM_MANAGED' | 'APE_DTS_MANAGED';
type RequiredAcks = 'one' | 'all';
type MessageFormat = '' | 'avro' | 'json';
type JsonTemplate = '' | 'standard' | 'cloudcanal';
type OperationPurpose = 'DELETE_TASK' | 'CREATE_TOPIC' | 'PREPARE_PG';
type ListRuntimeAction = 'START' | 'STOP' | 'RESTART';

function restartConfigurationMatches(task: TaskSummary) {
  return (
    task.applied != null &&
    (task.draft === null ||
      (task.draft.artifactId === task.applied.artifactId &&
        JSON.stringify(task.draft.configuration) === JSON.stringify(task.applied.configuration)))
  );
}

const deletableObservedStates = new Set<TaskSummary['observedState']>([
  'BLOCKED',
  'FAILED',
  'STOPPED',
]);

const observedStateBadgeClassNames: Record<TaskSummary['observedState'], string> = {
  STOPPED:
    'border-slate-200 bg-slate-100 text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200',
  STARTING:
    'border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-900 dark:bg-sky-950/60 dark:text-sky-300',
  RUNNING:
    'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900 dark:bg-emerald-950/60 dark:text-emerald-300',
  STOPPING:
    'border-amber-200 bg-amber-50 text-amber-800 dark:border-amber-900 dark:bg-amber-950/60 dark:text-amber-300',
  FAILED:
    'border-red-200 bg-red-50 text-red-700 dark:border-red-900 dark:bg-red-950/60 dark:text-red-300',
  BLOCKED:
    'border-orange-200 bg-orange-50 text-orange-800 dark:border-orange-900 dark:bg-orange-950/60 dark:text-orange-300',
  DELETED:
    'border-slate-200 bg-slate-100 text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-400',
};

interface TaskEditorDraft {
  readonly groupId: string | null;
  readonly artifactId: string | null;
  readonly ackIntervalSeconds: number | null;
  readonly batchSize: number | null;
  readonly bufferSize: number | null;
  readonly checkpointIntervalSeconds: number | null;
  readonly includeSnapshot: boolean;
  readonly messageFormat: MessageFormat;
  readonly jsonTemplate: JsonTemplate;
  readonly withFieldDefs: boolean | null;
  readonly name: string;
  readonly publicationName: string;
  readonly requiredAcks: RequiredAcks;
  readonly resourceMode: ResourceMode;
  readonly slotName: string;
  readonly sourceDataSourceId: string;
  readonly mappings: readonly TaskMappingDraft[];
  readonly targetDataSourceId: string;
  readonly taskType: TaskType;
}

const taskGuardCopy = {
  description: '离开后，当前页面尚未保存的任务配置将被清除。已成功保存到服务端的草稿不受影响。',
  title: '放弃未保存的任务配置？',
};

const checkLabels: Record<TaskPreflightCheckKey, string> = {
  ARTIFACT: '运行制品',
  CAPACITY: '运行容量',
  CONFIGURATION: '任务配置',
  DATA_SOURCE_ACCESS: '数据源权限',
  PG_RESOURCES: 'PostgreSQL 资源',
  MYSQL_CDC: 'MySQL Binlog',
  RUNTIME_ENVIRONMENT: '运行环境',
  PRIMARY_KEY: '主键条件',
  SOURCE_CONNECTION: '源端连接',
  SOURCE_METADATA: '源端元数据',
  TARGET_CONNECTION: '目标端连接',
  TARGET_METADATA: '目标端元数据',
  TOPIC_IDENTITIES: 'Topic 身份',
};

function newDraft(taskType: TaskType = 'PG_KAFKA'): TaskEditorDraft {
  return {
    groupId: null,
    artifactId: null,
    ackIntervalSeconds: 5,
    batchSize: 100,
    bufferSize: 16_000,
    checkpointIntervalSeconds: 15,
    includeSnapshot: false,
    messageFormat: '',
    jsonTemplate: '',
    withFieldDefs: null,
    name: '',
    publicationName: '',
    requiredAcks: 'all',
    resourceMode: 'PLATFORM_MANAGED',
    slotName: '',
    sourceDataSourceId: '',
    mappings: [emptyTaskMapping()],
    targetDataSourceId: '',
    taskType,
  };
}

function record(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function text(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

function positiveNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : null;
}

function draftFromTask(task: TaskSummary): TaskEditorDraft {
  const revision = task.draft ?? task.applied;
  const configuration = record(revision?.configuration);
  const source = record(configuration.source);
  const target = record(configuration.target);
  const tuning = record(configuration.tuning);
  const base = newDraft(task.taskType);
  return {
    ...base,
    groupId: task.groupId ?? null,
    artifactId: revision?.artifactId ?? null,
    ackIntervalSeconds: positiveNumber(tuning.ackIntervalSeconds),
    batchSize: positiveNumber(tuning.batchSize),
    bufferSize: positiveNumber(tuning.bufferSize),
    checkpointIntervalSeconds: positiveNumber(tuning.checkpointIntervalSeconds),
    includeSnapshot: source.includeSnapshot === true,
    messageFormat:
      target.messageFormat === 'avro' || target.messageFormat === 'json'
        ? target.messageFormat
        : '',
    jsonTemplate:
      target.jsonTemplate === 'standard' || target.jsonTemplate === 'cloudcanal'
        ? target.jsonTemplate
        : '',
    withFieldDefs: typeof target.withFieldDefs === 'boolean' ? target.withFieldDefs : null,
    name: task.name,
    publicationName: text(source.publicationName),
    requiredAcks: tuning.requiredAcks === 'one' ? 'one' : 'all',
    resourceMode:
      source.resourceMode === 'SELF_MANAGED' || source.resourceMode === 'APE_DTS_MANAGED'
        ? source.resourceMode
        : 'PLATFORM_MANAGED',
    slotName: text(source.slotName),
    sourceDataSourceId: task.sourceDataSourceId,
    mappings: readTaskMappings(configuration),
    targetDataSourceId: task.targetDataSourceId,
    taskType: task.taskType,
  };
}

function requireText(value: string, label: string): string {
  const normalized = value.trim();
  if (normalized.length === 0) throw new Error(`请填写${label}。`);
  return normalized;
}

function requirePattern(value: string, label: string, pattern: RegExp): string {
  const normalized = requireText(value, label);
  if (!pattern.test(normalized)) throw new Error(`${label}格式不正确。`);
  return normalized;
}

function optionalPattern(value: string, label: string, pattern: RegExp): string | undefined {
  const normalized = value.trim();
  if (normalized.length === 0) return undefined;
  if (!pattern.test(normalized)) throw new Error(`${label}格式不正确。`);
  return normalized;
}

function requirePositive(value: number | null, label: string): number {
  if (value === null || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${label}必须是正整数。`);
  }
  return value;
}

function optionalPositive(value: number | null, label: string): number | undefined {
  if (value === null) return undefined;
  return requirePositive(value, label);
}

function buildConfiguration(draft: TaskEditorDraft): Record<string, unknown> {
  const sourceDataSourceId = requireText(draft.sourceDataSourceId, '源数据源');
  const targetDataSourceId = requireText(draft.targetDataSourceId, '目标数据源');
  const mappings = serializeTaskMappings(draft.mappings, draft.taskType);
  const batchSize = optionalPositive(draft.batchSize, '批次大小');
  const bufferSize = optionalPositive(draft.bufferSize, '缓冲区大小');
  const checkpointIntervalSeconds = optionalPositive(draft.checkpointIntervalSeconds, '检查点间隔');
  const tuning = {
    ...(batchSize === undefined ? {} : { batchSize }),
    ...(bufferSize === undefined ? {} : { bufferSize }),
    ...(checkpointIntervalSeconds === undefined ? {} : { checkpointIntervalSeconds }),
    requiredAcks: draft.requiredAcks,
  };
  if (draft.taskType === 'PG_KAFKA') {
    if (draft.resourceMode === 'APE_DTS_MANAGED' && draft.includeSnapshot) {
      throw new Error('ape-dts 管理资源模式不支持初始快照。');
    }
    const publicationName = optionalPattern(
      draft.publicationName,
      'Publication 名称',
      /^[A-Za-z_][A-Za-z0-9_]{0,62}$/u,
    );
    const slotName = optionalPattern(draft.slotName, 'Slot 名称', /^[a-z0-9_]{1,63}$/u);
    return {
      kind: 'PG_TO_KAFKA',
      schemaVersion: 2,
      mappings,
      source: {
        dataSourceId: sourceDataSourceId,
        includeSnapshot: draft.includeSnapshot,
        ...(publicationName === undefined ? {} : { publicationName }),
        resourceMode: draft.resourceMode,
        ...(slotName === undefined ? {} : { slotName }),
      },
      target: {
        dataSourceId: targetDataSourceId,
        ...(draft.messageFormat === '' ? {} : { messageFormat: draft.messageFormat }),
        ...(draft.messageFormat !== 'json' || draft.jsonTemplate === ''
          ? {}
          : { jsonTemplate: draft.jsonTemplate }),
        ...(draft.withFieldDefs === null ? {} : { withFieldDefs: draft.withFieldDefs }),
      },
      tuning,
    };
  }
  if (draft.taskType === 'MYSQL_KAFKA') {
    return {
      kind: 'MYSQL_TO_KAFKA',
      schemaVersion: 2,
      mappings,
      source: { dataSourceId: sourceDataSourceId, includeSnapshot: draft.includeSnapshot },
      target: {
        dataSourceId: targetDataSourceId,
        ...(draft.messageFormat === '' ? {} : { messageFormat: draft.messageFormat }),
        ...(draft.messageFormat !== 'json' || draft.jsonTemplate === ''
          ? {}
          : { jsonTemplate: draft.jsonTemplate }),
        ...(draft.withFieldDefs === null ? {} : { withFieldDefs: draft.withFieldDefs }),
      },
      tuning,
    };
  }
  const ackIntervalSeconds = optionalPositive(draft.ackIntervalSeconds, 'ACK 间隔');
  return {
    kind: 'KAFKA_RAW_FORWARD',
    schemaVersion: 2,
    mappings,
    source: {
      dataSourceId: sourceDataSourceId,
    },
    target: {
      dataSourceId: targetDataSourceId,
    },
    tuning: {
      ...tuning,
      ...(ackIntervalSeconds === undefined ? {} : { ackIntervalSeconds }),
    },
  };
}

function taskTypeLabel(taskType: TaskType): string {
  return taskType === 'PG_KAFKA'
    ? 'PostgreSQL → Kafka'
    : taskType === 'MYSQL_KAFKA'
      ? 'MySQL → Kafka'
      : 'Kafka → Kafka Raw';
}

const selectStyle =
  'flex h-9 w-full rounded-md border border-input bg-background px-3 text-sm shadow-xs focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50';

function Field({ label, children, help }: { label: string; children: ReactNode; help?: string }) {
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2 text-sm font-medium">
        {label}
        {help && <Help label={`${label}说明`} text={help} />}
      </div>
      {children}
    </div>
  );
}

function Help({ label, text }: { label: string; text: string }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            aria-label={label}
            className="inline-flex text-muted-foreground hover:text-foreground"
          >
            <Info className="size-4" />
          </button>
        </TooltipTrigger>
        <TooltipContent className="max-w-sm">{text}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

function TaskPrompt({
  title,
  open,
  busy,
  onClose,
  children,
}: {
  title: string;
  open: boolean;
  busy: boolean;
  onClose: () => void;
  children: ReactNode;
}) {
  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next && !busy) onClose();
      }}
    >
      <DialogContent
        aria-describedby={undefined}
        showCloseButton={!busy}
        onInteractOutside={(event) => event.preventDefault()}
        onEscapeKeyDown={(event) => {
          if (busy) event.preventDefault();
        }}
      >
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
        </DialogHeader>
        {children}
      </DialogContent>
    </Dialog>
  );
}

function PreflightResult({ run, pgAction }: { run: TaskPreflightRunSummary; pgAction: ReactNode }) {
  const failed = run.result.checks.filter((check) => check.state === 'FAILED');
  const remaining = run.result.checks.filter((check) => check.state !== 'FAILED');
  const renderCheck = (check: TaskPreflightRunSummary['result']['checks'][number]) => (
    <div key={check.key} role="listitem" className="flex items-start justify-between gap-4 py-3">
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-medium">{checkLabels[check.key]}</span>
          {check.state === 'PASSED' && (
            <Help label={`${checkLabels[check.key]}检查说明`} text={check.summary} />
          )}
        </div>
        {check.state !== 'PASSED' && (
          <p className="mt-1 break-words text-sm text-muted-foreground">{check.summary}</p>
        )}
        {check.state === 'FAILED' && (
          <details className="mt-2 text-xs text-muted-foreground">
            <summary className="cursor-pointer">诊断代码</summary>
            <code className="mt-1 block break-all">{check.code}</code>
          </details>
        )}
        {check.key === 'PG_RESOURCES' && check.state === 'FAILED' && pgAction}
      </div>
      {check.state === 'PASSED' ? (
        <CircleCheck
          className="size-4 shrink-0 text-emerald-600 dark:text-emerald-400"
          aria-label="已通过"
        />
      ) : (
        <Badge variant={check.state === 'FAILED' ? 'destructive' : 'secondary'}>
          {check.state === 'FAILED' ? '未通过' : '已跳过'}
        </Badge>
      )}
    </div>
  );
  return (
    <Card aria-label="最近一次预检">
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle>最近一次预检</CardTitle>
        <Badge variant={run.state === 'PASSED' ? 'secondary' : 'destructive'}>
          {run.state === 'PASSED'
            ? '已通过'
            : run.state === 'FAILED'
              ? '未通过'
              : run.state === 'STALE'
                ? '已过期'
                : '检查中'}
        </Badge>
      </CardHeader>
      <CardContent className="space-y-4">
        {run.state === 'STALE' && (
          <p role="alert" className="text-sm text-destructive">
            预检结果已过期，请重新预检。
          </p>
        )}
        <p className="text-sm">
          共 {run.result.checks.length} 项，{failed.length} 项未通过，
          {run.result.checks.filter((check) => check.state === 'PASSED').length} 项通过。
        </p>
        {failed.length > 0 && (
          <div role="list" aria-label="未通过检查" className="divide-y">
            {failed.map(renderCheck)}
          </div>
        )}
        {remaining.length > 0 && (
          <details>
            <summary className="cursor-pointer text-sm text-muted-foreground">
              查看通过及跳过的检查（{remaining.length}）
            </summary>
            <div role="list" className="divide-y">
              {remaining.map(renderCheck)}
            </div>
          </details>
        )}
        <dl className="grid gap-4 border-t pt-4 sm:grid-cols-2">
          <div>
            <dt className="text-sm text-muted-foreground">所需运行单元</dt>
            <dd>{run.result.capacity.requested ?? '未知'}</dd>
          </div>
          <div>
            <dt className="text-sm text-muted-foreground">可用运行单元</dt>
            <dd>{run.result.capacity.available ?? '未知'}</dd>
          </div>
        </dl>
      </CardContent>
    </Card>
  );
}

export function TaskManagementPage({
  initialDetailTaskId,
  onLeaveGuardChange,
  onOpenDataSources,
  onSessionInvalidated,
  session,
}: {
  initialDetailTaskId?: string | undefined;
  onLeaveGuardChange?: (guard: LeaveGuard | null) => void;
  onOpenDataSources?: () => void;
  onSessionInvalidated: () => void;
  session: AuthenticatedSessionResponse;
}) {
  const [query, setQuery] = useState('');
  const [listPage, setListPage] = useState({ current: 1, pageSize: 10 });
  const listViewport = useRef<HTMLDivElement>(null);
  const initialDetailId = useRef(initialDetailTaskId);
  const [detailTask, setDetailTask] = useState<TaskSummary | null>(null);
  const [discardTarget, setDiscardTarget] = useState<TaskSummary | null>(null);
  const [recoveryAction, setRecoveryAction] = useState<'RETRY' | 'DISCARD'>('DISCARD');
  const [discardConfirmed, setDiscardConfirmed] = useState(false);
  const [discarding, setDiscarding] = useState(false);
  const discardInFlight = useRef(false);
  const discardRequest = useRef(0);
  const [pendingTaskRefresh, setPendingTaskRefresh] = useState<readonly string[]>([]);
  const detailHeading = useRef<HTMLHeadingElement>(null);
  const [tasks, setTasks] = useState<readonly TaskSummary[]>([]);
  const [groups, setGroups] = useState<readonly TaskGroupSummary[]>([]);
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>('ALL');
  const [groupsError, setGroupsError] = useState(false);
  const [groupsLoading, setGroupsLoading] = useState(true);
  const [groupPrompt, setGroupPrompt] = useState<'LIST' | 'EDITOR' | 'RENAME' | null>(null);
  const [renamingGroup, setRenamingGroup] = useState<TaskGroupSummary | null>(null);
  const [deletingGroup, setDeletingGroup] = useState<{
    group: TaskGroupSummary;
    occupied: boolean;
  } | null>(null);
  const [groupName, setGroupName] = useState('');
  const [groupBusy, setGroupBusy] = useState(false);
  const groupPending = useRef(false);
  const groupListRequest = useRef(0);
  const [autoRefreshList, setAutoRefreshList] = useState(false);
  const [listRefreshInterval, setListRefreshInterval] = useState(15_000);
  const [refreshingList, setRefreshingList] = useState(false);
  const listLoadPending = useRef(false);
  const listLoadGeneration = useRef(0);
  const progress = useTaskProgress(
    tasks.map((task) => task.id),
    onSessionInvalidated,
    null,
  );
  const refreshProgress = progress.refresh;
  const [dataSources, setDataSources] = useState<readonly DataSourceSummary[]>([]);
  const [artifacts, setArtifacts] = useState<readonly ArtifactSummary[]>([]);
  const [artifactError, setArtifactError] = useState<string | null>(null);
  const [artifactReload, setArtifactReload] = useState(0);
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [editorOpen, setEditorOpen] = useState(false);
  const [step, setStep] = useState(0);
  const [topicPrompt, setTopicPrompt] = useState(false);
  const [selectedTopic, setSelectedTopic] = useState<TaskMappingTarget | null>(null);
  const [prepareTopicOpen, setPrepareTopicOpen] = useState(false);
  const [preparePgOpen, setPreparePgOpen] = useState(false);
  const [pgRiskConfirmed, setPgRiskConfirmed] = useState(false);
  const [preparingPg, setPreparingPg] = useState(false);
  const [pgFeedback, setPgFeedback] = useState<{
    readonly context: string;
    readonly message: string;
    readonly failed: boolean;
  } | null>(null);
  const pgRequest = useRef(0);
  const pgContext = useRef('');
  const [creatingTopic, setCreatingTopic] = useState(false);
  const topicRequest = useRef(0);
  const topicContext = useRef('');
  const [topicRefresh, setTopicRefresh] = useState(0);
  const [draft, setDraft] = useState<TaskEditorDraft>(() => newDraft());
  const [baseline, setBaseline] = useState<TaskEditorDraft>(() => newDraft());
  const [preflight, setPreflight] = useState<TaskPreflightRunSummary | null>(null);
  const [starting, setStarting] = useState(false);
  const [listRuntimePrompt, setListRuntimePrompt] = useState<{
    task: TaskSummary;
    action: 'STOP' | 'RESTART';
  } | null>(null);
  const [listRuntimeBusy, setListRuntimeBusy] = useState(false);
  const listRuntimePending = useRef(false);
  const listRuntimeGeneration = useRef(0);
  const startPending = useRef(false);
  const [reinitializeTarget, setReinitializeTarget] = useState<TaskSummary | null>(null);
  const reinitializeRequest = useRef(0);
  const preflightRequest = useRef(0);
  const preflightContext = useRef(0);
  useEffect(
    () => () => {
      reinitializeRequest.current += 1;
      preflightRequest.current += 1;
      listRuntimeGeneration.current += 1;
    },
    [],
  );
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleteTargetId, setDeleteTargetId] = useState<string | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const savePending = useRef(false);
  const saveRequest = useRef(0);
  const saveGeneration = useRef(0);
  const saveContext = useRef('');
  const [pgAddition, setPgAddition] = useState<{
    readonly context: string;
    readonly generation: number;
    readonly task: TaskSummary;
    readonly configuration: Record<string, unknown>;
    readonly draft: TaskEditorDraft;
    readonly addedTables: readonly { schema: string; name: string }[];
  } | null>(null);
  const [checking, setChecking] = useState(false);
  const { holder, setError, setNotice, notify } = useOperationNotification();
  const dirty = JSON.stringify(draft) !== JSON.stringify(baseline);
  const { prompt, requestLeave } = useDraftGuard(
    dirty,
    saving || preparingPg || groupBusy,
    taskGuardCopy,
  );
  const selectedTask = tasks.find((task) => task.id === selectedTaskId) ?? null;
  const visibleTasks = tasks.filter(
    (task) =>
      (selectedGroupId === 'ALL' || (task.groupId ?? null) === selectedGroupId) &&
      task.name.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()),
  );
  const currentListPage = Math.min(
    listPage.current,
    Math.max(1, Math.ceil(visibleTasks.length / listPage.pageSize)),
  );
  useEffect(() => {
    if (listPage.current !== currentListPage) {
      setListPage((page) => ({ ...page, current: currentListPage }));
    }
  }, [currentListPage, listPage]);
  const pagedTasks = visibleTasks.slice(
    (currentListPage - 1) * listPage.pageSize,
    currentListPage * listPage.pageSize,
  );
  useEffect(() => {
    if (listViewport.current) listViewport.current.scrollTop = 0;
  }, [currentListPage, listPage.pageSize, selectedGroupId, query]);
  const currentSaveContext = JSON.stringify([
    editorOpen,
    selectedTaskId,
    selectedTask?.rowVersion,
    selectedTask?.draft?.id,
    selectedTask?.draft?.draftVersion,
    selectedTask?.currentAppliedRevisionId,
    draft,
  ]);
  saveContext.current = currentSaveContext;
  useEffect(() => {
    setPgAddition(null);
  }, [currentSaveContext]);
  const reinitializationPreflight =
    preflight?.taskId === selectedTask?.id &&
    preflight?.result.checks.some(
      (check) => check.key === 'CONFIGURATION' && check.details.reinitializeMysqlSnapshot === true,
    );
  const deleteTarget = tasks.find((task) => task.id === deleteTargetId) ?? null;
  const postgresSources = dataSources.filter(
    (source) => source.state === 'ACTIVE' && source.configuration.type === 'POSTGRES',
  );
  const kafkaSources = dataSources.filter(
    (source) => source.state === 'ACTIVE' && source.configuration.type === 'KAFKA',
  );
  const mysqlSources = dataSources.filter(
    (source) => source.state === 'ACTIVE' && source.configuration.type === 'MYSQL',
  );
  const sourceOptions =
    draft.taskType === 'PG_KAFKA'
      ? postgresSources
      : draft.taskType === 'MYSQL_KAFKA'
        ? mysqlSources
        : kafkaSources;
  const sourceDataSource =
    dataSources.find((source) => source.id === draft.sourceDataSourceId) ?? null;
  const targetDataSource =
    dataSources.find((source) => source.id === draft.targetDataSourceId) ?? null;
  const sourceDatabase =
    sourceDataSource?.configuration.type === 'POSTGRES' ||
    sourceDataSource?.configuration.type === 'MYSQL'
      ? sourceDataSource.configuration.database
      : undefined;

  const currentTopicContext = JSON.stringify([
    editorOpen,
    selectedTaskId,
    draft.targetDataSourceId,
    draft.mappings,
    selectedTopic,
  ]);
  topicContext.current = currentTopicContext;
  const currentPgContext = JSON.stringify([editorOpen, selectedTaskId, draft]);
  pgContext.current = currentPgContext;
  const savedPgAddition = pgTableAddition(
    selectedTask?.applied?.configuration,
    selectedTask?.draft?.configuration,
  );
  const pgExpansion =
    savedPgAddition.eligible &&
    record(record(selectedTask?.draft?.configuration).source).includeSnapshot === false;
  const canPreparePg =
    session.user.role === 'ADMIN' &&
    draft.taskType === 'PG_KAFKA' &&
    draft.resourceMode !== 'APE_DTS_MANAGED' &&
    selectedTask !== null &&
    selectedTask.draft !== null &&
    selectedTask.desiredState === 'STOPPED' &&
    selectedTask.observedState === 'STOPPED' &&
    (selectedTask.currentAppliedRevisionId === null || pgExpansion) &&
    !dirty &&
    !saving &&
    !checking &&
    !preparingPg;
  const pgPreparationGuidance =
    preparingPg || canPreparePg
      ? null
      : dirty || selectedTask?.draft == null
        ? '请先保存当前草稿。'
        : selectedTask.currentAppliedRevisionId !== null && !pgExpansion
          ? '已有运行任务仅支持确认后的增量加表；请保留原复制槽、发布及旧表映射。'
          : ['BLOCKED', 'FAILED'].includes(selectedTask.observedState)
            ? '该任务已有失败运行记录，不能重新执行首次资源准备；请复制为新任务，并使用新的 Slot 和 Publication 名称。'
            : selectedTask.desiredState !== 'STOPPED' || selectedTask.observedState !== 'STOPPED'
              ? '任务停止后可准备复制资源。'
              : saving || checking
                ? '请等待当前保存或预检完成。'
                : null;
  const topicPending =
    preflight?.result.checks.some(
      (check) => check.state === 'FAILED' && check.code === 'TARGET_TOPIC_CREATION_PENDING',
    ) === true;
  const topicPreparationBlocked =
    dirty || selectedTask?.draft == null
      ? '请先保存当前草稿并重新预检。'
      : selectedTask.currentAppliedRevisionId !== null && !pgExpansion
        ? '已有运行修订，不支持首次 Topic 准备。'
        : selectedTask.desiredState !== 'STOPPED' || selectedTask.observedState !== 'STOPPED'
          ? '任务停止后可准备 Topic。'
          : saving || checking || starting || preparingPg
            ? '请等待当前操作完成。'
            : preflight?.result.checks.filter((check) => check.state === 'FAILED').length !== 1
              ? '请先处理其他未通过的检查，再准备 Topic。'
              : preflight.state !== 'FAILED' ||
                  preflight.completedAt === null ||
                  preflight.revisionId !== selectedTask.draft.id ||
                  preflight.result.taskRevision.draftVersion !== selectedTask.draft.draftVersion
                ? '预检结果不完整或已过期，请重新预检。'
                : null;
  useEffect(
    () => () => {
      topicRequest.current += 1;
      pgRequest.current += 1;
      discardRequest.current += 1;
      saveRequest.current += 1;
    },
    [],
  );

  useEffect(() => {
    onLeaveGuardChange?.(requestLeave);
    return () => onLeaveGuardChange?.(null);
  }, [onLeaveGuardChange, requestLeave]);

  const reportFailure = useCallback(
    (prefix: string, caught: unknown, purpose?: OperationPurpose) => {
      const showError = (message: string) => {
        if (purpose === 'PREPARE_PG') {
          setPgFeedback({ context: pgContext.current, message, failed: true });
        } else {
          setError(message);
        }
      };
      if (caught instanceof PlatformApiError) {
        if (caught.code === 'TASK_PG_TABLE_ADDITION_CONFIRMATION_REQUIRED') {
          showError('新增来源表的确认已失效，请重新核对当前配置并确认保存。');
          return;
        }
        if (caught.code === 'TASK_PG_TABLE_ADDITION_NOT_ALLOWED') {
          showError(
            '当前配置不支持增量加表，请保留原连接、复制资源和旧表映射，并确认原任务未包含未完成的初始快照。',
          );
          return;
        }
        if (caught.code === 'RUNTIME_EXTERNAL_IDENTITY_MISMATCH') {
          const descriptions: Record<string, string> = {
            KAFKA_IDENTITY_MISSING: '缺少 Kafka 集群身份信息，请检查连接并重新预检',
            DATA_SOURCE_IDENTITY_CHANGED: '数据源集群身份与预检记录不一致，请核对连接并重新预检',
            KAFKA_PARTITION_BASELINE_MISSING: '缺少有效的来源 Topic 分区信息，请重新预检',
            KAFKA_PARTITION_SET_CHANGED: '来源 Topic 分区与预检记录不一致，请重新预检',
          };
          showError(
            `${prefix}：${descriptions[caught.reason ?? ''] ?? '外部资源绑定与预检记录不一致，请核对资源并重新预检'}（${caught.code}）。`,
          );
          return;
        }
        if (caught.status === 401 || caught.code === 'SESSION_INVALID') {
          onSessionInvalidated();
          return;
        }
        if (caught.code === 'KAFKA_TOPIC_AUDIT_FAILED') {
          setTopicRefresh((value) => value + 1);
          setError('Topic 操作已完成，但审计记录写入失败。请刷新目标 Topic 核实结果，勿重复创建。');
          return;
        }
        if (caught.code === 'RUNTIME_TOPIC_PREPARATION_FAILED') {
          setTopicRefresh((value) => value + 1);
          setError('Topic 准备未完成，Topic 可能已创建；请刷新核实，任务未启动。');
          return;
        }
        if (purpose === 'PREPARE_PG') {
          const descriptions: Record<string, string> = {
            PG_RESOURCE_PREPARATION_FAILED:
              '复制资源准备未完成，发布可能已创建或扩展并保留，但新配置尚未应用；请检查源库后重试准备并预检，任务未启动',
            PG_RESOURCE_CONFLICT:
              '已有复制资源与当前任务不兼容，未修改已有资源，请检查名称、归属和配置',
            PG_RESOURCE_NOT_FOUND: '未找到指定的已有复制资源，请先创建或选择平台管理方式',
            PG_RESOURCE_ENGINE_MANAGED: '引擎管理模式不使用平台准备入口',
            PG_RESOURCE_VERSION_CONFLICT: '任务草稿已变化，请重新打开最新任务后确认',
            PG_RESOURCE_PREPARATION_NOT_ALLOWED:
              '任务须停止后准备资源；历史任务仅支持已确认的增量加表，请核对当前草稿与原复制资源',
          };
          if (descriptions[caught.code]) {
            showError(`${descriptions[caught.code]}（${caught.code}）。`);
            return;
          }
        }
        if (purpose === 'CREATE_TOPIC') {
          const descriptions: Record<string, string> = {
            KAFKA_TOPIC_PARAMETERS_INVALID: '分区数或副本数不符合目标集群要求',
            PERMISSION_DENIED: '当前连接账户没有 Topic 创建权限，请检查 Kafka ACL',
            TIMEOUT: '创建请求超时，结果尚未确认；请刷新 Topic 核实后再决定是否重试',
          };
          if (descriptions[caught.code]) {
            setError(`${prefix}：${descriptions[caught.code]}（${caught.code}）。`);
            return;
          }
        }
        showError(
          `${prefix}：${operationName(caught.code)}${operationName(caught.code) === caught.code ? '' : `（${caught.code}）`}`,
        );
        return;
      }
      showError(caught instanceof Error ? caught.message : `${prefix}。`);
    },
    [onSessionInvalidated, setError],
  );

  function invalidateListLoad() {
    listLoadGeneration.current += 1;
    listLoadPending.current = false;
    setRefreshingList(false);
  }

  const load = useCallback(async () => {
    if (listLoadPending.current) return;
    listLoadPending.current = true;
    const generation = ++listLoadGeneration.current;
    setRefreshingList(true);
    setError(null);
    try {
      const [taskResult, sourceResult] = await Promise.all([
        tasksApi.list(),
        dataSourcesApi.list(),
      ]);
      if (generation !== listLoadGeneration.current) return;
      setTasks(taskResult.tasks);
      setPendingTaskRefresh([]);
      setDataSources(sourceResult.dataSources);
      if (initialDetailId.current !== undefined) {
        const requested = taskResult.tasks.find((task) => task.id === initialDetailId.current);
        initialDetailId.current = undefined;
        if (requested) setDetailTask(requested);
        else setError('任务已不存在或当前账号无权查看，请刷新任务列表。');
      }
    } catch (caught) {
      if (generation === listLoadGeneration.current) reportFailure('读取任务失败', caught);
    } finally {
      if (generation === listLoadGeneration.current) {
        listLoadPending.current = false;
        setLoading(false);
        setRefreshingList(false);
      }
    }
  }, [reportFailure, setError]);

  useEffect(() => {
    void load();
    return () => {
      listLoadGeneration.current += 1;
      listLoadPending.current = false;
    };
  }, [load]);

  const loadGroups = useCallback(async () => {
    const generation = listRuntimeGeneration.current;
    const request = ++groupListRequest.current;
    setGroupsLoading(true);
    setGroupsError(false);
    try {
      const result = await taskGroupsApi.list();
      if (generation === listRuntimeGeneration.current && request === groupListRequest.current) {
        setGroups(result.groups);
        setSelectedGroupId((current) =>
          current === 'ALL' || result.groups.some((group) => group.id === current)
            ? current
            : 'ALL',
        );
      }
    } catch (caught) {
      if (generation !== listRuntimeGeneration.current || request !== groupListRequest.current)
        return;
      setGroupsError(true);
      if (caught instanceof PlatformApiError && caught.status === 401) onSessionInvalidated();
    } finally {
      if (generation === listRuntimeGeneration.current && request === groupListRequest.current)
        setGroupsLoading(false);
    }
  }, [onSessionInvalidated]);

  const refreshList = useCallback(async () => {
    await Promise.all([load(), refreshProgress(), loadGroups()]);
  }, [load, refreshProgress, loadGroups]);

  useEffect(() => {
    if (
      !autoRefreshList ||
      editorOpen ||
      detailTask !== null ||
      listRuntimeBusy ||
      groupBusy ||
      groupPrompt !== null ||
      deletingGroup !== null ||
      deleteOpen ||
      deleting ||
      reinitializeTarget !== null ||
      starting
    )
      return;
    let active = true;
    let timer = setTimeout(() => void poll(), listRefreshInterval);
    async function poll() {
      await refreshList();
      if (active) timer = setTimeout(() => void poll(), listRefreshInterval);
    }
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, [
    autoRefreshList,
    editorOpen,
    detailTask,
    listRuntimeBusy,
    groupBusy,
    groupPrompt,
    deletingGroup,
    deleteOpen,
    deleting,
    reinitializeTarget,
    starting,
    listRefreshInterval,
    refreshList,
  ]);

  useEffect(() => {
    void loadGroups();
  }, [loadGroups]);

  async function assignGroup(taskId: string, groupId: string | null) {
    if (groupPending.current || listRuntimePending.current || savePending.current) return;
    const target = tasks.find((task) => task.id === taskId);
    if (target === undefined || (target.groupId ?? null) === groupId) return;
    groupPending.current = true;
    setGroupBusy(true);
    const generation = listRuntimeGeneration.current;
    invalidateListLoad();
    try {
      const saved = await taskGroupsApi.assign(target, groupId);
      if (generation !== listRuntimeGeneration.current) return;
      // 仅合并分组字段，避免覆盖分配期间更新的运行状态或配置。
      setTasks((current) =>
        current.map((task) =>
          task.id === saved.id ? { ...task, groupId: saved.groupId ?? null } : task,
        ),
      );
      setNotice(groupId === null ? '任务已取消分组，可在全部任务中查看。' : '任务分组已更新。');
    } catch (caught) {
      if (generation !== listRuntimeGeneration.current) return;
      reportFailure('分配任务组失败', caught);
      await load();
    } finally {
      groupPending.current = false;
      setGroupBusy(false);
    }
  }

  function openGroupPrompt(origin: 'LIST' | 'EDITOR') {
    setRenamingGroup(null);
    setGroupName('');
    setGroupPrompt(origin);
  }

  async function createGroup() {
    const name = groupName.trim();
    if (!name || groupPending.current || savePending.current) return;
    groupPending.current = true;
    groupListRequest.current += 1;
    setGroupsLoading(false);
    setGroupBusy(true);
    const origin = groupPrompt;
    const targetId = selectedTaskId;
    const generation = saveGeneration.current;
    const pageGeneration = listRuntimeGeneration.current;
    let created: TaskGroupSummary | undefined;
    try {
      if (origin === 'RENAME' && renamingGroup) {
        const renamed = await taskGroupsApi.rename(renamingGroup, name);
        if (pageGeneration !== listRuntimeGeneration.current) return;
        setGroups((current) => current.map((group) => (group.id === renamed.id ? renamed : group)));
        setGroupPrompt(null);
        setNotice('任务组已重命名。');
        return;
      }
      created = await taskGroupsApi.create(name);
      if (pageGeneration !== listRuntimeGeneration.current) return;
      const group = created;
      setGroups((current) => [...current, group]);
      setGroupPrompt(null);
      setNotice('任务组已创建。');
    } catch (caught) {
      if (pageGeneration === listRuntimeGeneration.current) {
        reportFailure(origin === 'RENAME' ? '重命名任务组失败' : '创建任务组失败', caught);
        if (
          origin === 'RENAME' &&
          caught instanceof PlatformApiError &&
          ['TASK_GROUP_CHANGED', 'TASK_GROUP_NOT_FOUND'].includes(caught.code)
        ) {
          setGroupPrompt(null);
          setRenamingGroup(null);
        }
        await loadGroups();
      }
    } finally {
      groupPending.current = false;
      setGroupBusy(false);
    }
    if (created === undefined) return;
    if (origin === 'LIST') setSelectedGroupId(created.id);
    else if (origin === 'EDITOR' && generation === saveGeneration.current) {
      if (targetId === null) update('groupId', created.id);
      else await assignGroup(targetId, created.id);
    }
  }

  async function reorderGroups(ids: string[]) {
    if (groupPending.current || groupsLoading || groupsError) return;
    groupPending.current = true;
    groupListRequest.current += 1;
    setGroupBusy(true);
    const generation = listRuntimeGeneration.current;
    try {
      const saved = await taskGroupsApi.reorder(
        ids,
        groups.map((group) => group.id),
      );
      if (generation !== listRuntimeGeneration.current) return;
      setGroups(saved.groups);
      setNotice('任务组顺序已保存。');
    } catch (caught) {
      if (generation === listRuntimeGeneration.current) {
        reportFailure('调整任务组顺序失败', caught);
        await loadGroups();
      }
    } finally {
      groupPending.current = false;
      setGroupBusy(false);
    }
  }

  async function deleteGroup() {
    if (!deletingGroup || groupPending.current) return;
    const { group, occupied } = deletingGroup;
    groupPending.current = true;
    groupListRequest.current += 1;
    setGroupBusy(true);
    invalidateListLoad();
    const generation = listRuntimeGeneration.current;
    try {
      await taskGroupsApi.delete(group, occupied);
      if (generation !== listRuntimeGeneration.current) return;
      setGroups((current) => current.filter((item) => item.id !== group.id));
      setTasks((current) =>
        current.map((task) => (task.groupId === group.id ? { ...task, groupId: null } : task)),
      );
      if (selectedGroupId === group.id) setSelectedGroupId('ALL');
      setDeletingGroup(null);
      setNotice('任务组已删除，组内任务已保留，可在全部任务中查看。');
    } catch (caught) {
      if (generation !== listRuntimeGeneration.current) return;
      if (caught instanceof PlatformApiError && caught.code === 'TASK_GROUP_NOT_EMPTY') {
        setDeletingGroup({ group, occupied: true });
      } else {
        reportFailure('删除任务组失败', caught);
        setDeletingGroup(null);
        await Promise.all([loadGroups(), load()]);
      }
    } finally {
      groupPending.current = false;
      setGroupBusy(false);
    }
  }

  useEffect(() => {
    if (!editorOpen && detailTask?.id === undefined) return;
    let active = true;
    setArtifactError(null);
    void listArtifacts()
      .then((result) => {
        if (active) setArtifacts(result.artifacts);
      })
      .catch((caught: unknown) => {
        if (!active) return;
        if (caught instanceof PlatformApiError && caught.status === 401) onSessionInvalidated();
        else setArtifactError('运行制品列表读取失败，已有绑定保持不变。');
      });
    return () => {
      active = false;
    };
  }, [editorOpen, detailTask?.id, onSessionInvalidated, artifactReload]);

  async function discardRecovery() {
    if (discardTarget === null || !discardConfirmed || discardInFlight.current) return;
    discardInFlight.current = true;
    const request = ++discardRequest.current;
    setDiscarding(true);
    setError(null);
    try {
      const recovered =
        recoveryAction === 'RETRY' ? await recoveryApi.retryTask(discardTarget) : null;
      if (recoveryAction === 'DISCARD') await tasksApi.discardRecovery(discardTarget);
      if (request !== discardRequest.current) return;
      setPendingTaskRefresh((current) => [...current, discardTarget.id]);
      setDiscardTarget(null);
      setDetailTask(null);
      if (
        recovered !== null &&
        'operation' in recovered &&
        recovered.operation.state !== 'SUCCEEDED'
      ) {
        setError(
          `任务恢复未完成：${recovered.operation.failureSummary ?? recovered.operation.failureCode ?? '请查看运行记录'}`,
        );
      } else if (recovered !== null && !('operation' in recovered) && recovered.state !== 'READY') {
        const step = recovered.steps.find((item) => ['FAILED', 'PAUSED'].includes(item.status));
        setError(`任务恢复未完成：${step?.errorSummary ?? step?.failureCode ?? '请查看运行记录'}`);
      } else {
        setNotice(
          recovered === null ? '已放弃旧运行恢复，请重新预检后启动任务。' : '任务已恢复运行。',
        );
      }
      try {
        const result = await tasksApi.list();
        if (request !== discardRequest.current) return;
        setTasks(result.tasks);
        setPendingTaskRefresh([]);
      } catch (caught) {
        if (request !== discardRequest.current) return;
        reportFailure('恢复操作已完成，但刷新任务列表失败，请刷新后继续操作', caught);
      }
    } catch (caught) {
      if (request === discardRequest.current)
        reportFailure(recoveryAction === 'RETRY' ? '恢复任务失败' : '放弃恢复失败', caught);
    } finally {
      discardInFlight.current = false;
      if (request === discardRequest.current) setDiscarding(false);
    }
  }

  function clearDeletePrompt() {
    setDeleteOpen(false);
    setDeleteTargetId(null);
  }

  function openDeletePrompt(task: TaskSummary) {
    setError(null);
    setNotice(null);
    setDeleteTargetId(task.id);
    setDeleteOpen(true);
  }

  function invalidatePreflight() {
    preflightContext.current += 1;
    setPreflight(null);
  }

  function update<K extends keyof TaskEditorDraft>(key: K, value: TaskEditorDraft[K]) {
    saveGeneration.current += 1;
    setPgAddition(null);
    setDraft((current) => ({ ...current, [key]: value }));
    setNotice(null);
    invalidatePreflight();
    clearDeletePrompt();
  }

  function selectTask(task: TaskSummary) {
    requestLeave(() => {
      invalidateListLoad();
      saveGeneration.current += 1;
      setPgAddition(null);
      const next = draftFromTask(task);
      setSelectedTaskId(task.id);
      setDraft(next);
      setBaseline(next);
      invalidatePreflight();
      clearDeletePrompt();
      setError(null);
      setNotice(null);
      setStep(0);
      setEditorOpen(true);
    });
  }

  function beginCreate() {
    requestLeave(() => {
      invalidateListLoad();
      saveGeneration.current += 1;
      setPgAddition(null);
      const next = { ...newDraft(), groupId: selectedGroupId === 'ALL' ? null : selectedGroupId };
      setSelectedTaskId(null);
      setDraft(next);
      setBaseline(next);
      invalidatePreflight();
      clearDeletePrompt();
      setError(null);
      setNotice(null);
      setStep(0);
      setEditorOpen(true);
    });
  }

  function closeEditor() {
    requestLeave(() => {
      saveGeneration.current += 1;
      setPgAddition(null);
      const next = newDraft();
      setEditorOpen(false);
      setSelectedTaskId(null);
      setDraft(next);
      setBaseline(next);
      invalidatePreflight();
      clearDeletePrompt();
      setError(null);
    });
  }

  function beginCopyFrom(task: TaskSummary) {
    requestLeave(() => {
      invalidateListLoad();
      saveGeneration.current += 1;
      setPgAddition(null);
      const empty = newDraft(task.taskType);
      setSelectedTaskId(null);
      setDraft({
        ...draftFromTask(task),
        name: `${task.name} 副本`.slice(0, 128),
      });
      setBaseline(empty);
      invalidatePreflight();
      clearDeletePrompt();
      setError(null);
      setNotice('已复制任务配置；预检、风险确认和运行状态不会复制。');
      setStep(0);
      setEditorOpen(true);
    });
  }

  function beginCopy() {
    if (selectedTask === null) return;
    requestLeave(() => {
      saveGeneration.current += 1;
      setPgAddition(null);
      const empty = newDraft(selectedTask.taskType);
      const copied = {
        ...draftFromTask(selectedTask),
        name: `${selectedTask.name} 副本`.slice(0, 128),
      };
      setSelectedTaskId(null);
      setDraft(copied);
      setBaseline(empty);
      invalidatePreflight();
      clearDeletePrompt();
      setError(null);
      setNotice('已复制任务配置；预检、风险确认和运行状态不会复制。');
    });
  }

  function topicParameters(selection = selectedTopic) {
    if (selection === null) throw new Error('请选择目标 Topic。');
    const topic = requirePattern(selection.topic, '目标 Topic', /^[A-Za-z0-9._-]{1,249}$/u);
    if (topic === '.' || topic === '..') throw new Error('目标 Topic 格式不正确。');
    return {
      topic,
      numPartitions: requirePositive(selection.numPartitions, '分区数'),
      replicationFactor: requirePositive(selection.replicationFactor, '副本数'),
    };
  }

  function prepareTopicCreation(selection: TaskMappingTarget) {
    setError(null);
    try {
      requireText(draft.targetDataSourceId, '目标数据源');
      topicParameters(selection);
      setSelectedTopic(selection);
      setTopicPrompt(true);
    } catch (caught) {
      reportFailure('创建 Topic 失败', caught);
    }
  }

  async function createTopic() {
    if (creatingTopic || targetDataSource === null || session.user.role !== 'ADMIN') return;
    const context = currentTopicContext;
    const request = ++topicRequest.current;
    setCreatingTopic(true);
    setError(null);
    setNotice(null);
    try {
      const result = await dataSourcesApi.createTopic(targetDataSource, topicParameters());
      if (request !== topicRequest.current || context !== topicContext.current) return;
      setTopicPrompt(false);
      setTopicRefresh((value) => value + 1);
      setNotice(
        result.created
          ? `Topic「${result.topic}」已创建。任务配置尚未保存。`
          : `Topic「${result.topic}」已存在，未修改分区或副本。`,
      );
    } catch (caught) {
      if (request === topicRequest.current && context === topicContext.current) {
        setTopicPrompt(false);
        reportFailure('创建 Topic 失败', caught, 'CREATE_TOPIC');
      }
    } finally {
      if (request === topicRequest.current) setCreatingTopic(false);
    }
  }

  async function persistDraft(
    configuration: Record<string, unknown>,
    taskToSave: TaskSummary | null,
    draftToSave: TaskEditorDraft,
    acknowledgePgTableAddition?: true,
  ) {
    if (savePending.current || groupPending.current) return;
    savePending.current = true;
    invalidateListLoad();
    const request = ++saveRequest.current;
    const context = saveContext.current;
    const generation = saveGeneration.current;
    const active = () =>
      request === saveRequest.current &&
      context === saveContext.current &&
      generation === saveGeneration.current;
    setSaving(true);
    setError(null);
    setNotice(null);
    try {
      let saved: TaskSummary;
      if (taskToSave === null) {
        const name = requirePattern(
          draftToSave.name,
          '任务名称',
          /^[\p{L}\p{N}][\p{L}\p{N} ._:-]{0,127}$/u,
        );
        saved = await tasksApi.create({
          ...(draftToSave.groupId === null ? {} : { groupId: draftToSave.groupId }),
          artifactId: draftToSave.artifactId,
          configuration,
          name,
          sourceDataSourceId: draftToSave.sourceDataSourceId,
          targetDataSourceId: draftToSave.targetDataSourceId,
          taskType: draftToSave.taskType,
        });
      } else {
        saved =
          acknowledgePgTableAddition === true
            ? await tasksApi.saveDraft(taskToSave, configuration, draftToSave.artifactId, true)
            : await tasksApi.saveDraft(taskToSave, configuration, draftToSave.artifactId);
      }
      if (request !== saveRequest.current) return;
      setTasks((current) => {
        const exists = current.some((task) => task.id === saved.id);
        return exists
          ? current.map((task) => (task.id === saved.id ? saved : task))
          : [...current, saved].sort((left, right) => left.name.localeCompare(right.name, 'zh-CN'));
      });
      if (!active()) return;
      const next = draftFromTask(saved);
      setSelectedTaskId(saved.id);
      setDraft(next);
      setBaseline(next);
      setNotice(taskToSave === null ? '任务已创建并保存为服务端草稿。' : '任务草稿已保存。');
    } catch (caught) {
      if (!active()) return;
      reportSaveFailure(caught);
    } finally {
      if (request === saveRequest.current) {
        savePending.current = false;
        setSaving(false);
        setPgAddition(null);
      }
    }
  }

  function reportSaveFailure(caught: unknown) {
    if (caught instanceof Error && !(caught instanceof PlatformApiError)) {
      setStep(
        /目标|分区|副本/u.test(caught.message)
          ? 1
          : /批次|缓冲|检查点|ACK/u.test(caught.message)
            ? 2
            : 0,
      );
    }
    reportFailure('保存任务失败', caught);
  }

  function save() {
    if (savePending.current) return;
    try {
      const configuration = buildConfiguration(draft);
      const addition = pgTableAddition(selectedTask?.applied?.configuration, configuration);
      if (selectedTask !== null && addition.addedTables.length > 0) {
        if (!addition.eligible) {
          throw new Error(
            record(selectedTask.applied?.configuration.source).includeSnapshot === true
              ? '原配置包含初始快照，当前无法确认旧表存量已完成，暂不支持增量加表。'
              : '新增表必须保留原连接、复制槽、Publication、旧表与目标 Topic；请撤销其他映射变更。',
          );
        }
        setError(null);
        setPgAddition({
          context: saveContext.current,
          generation: saveGeneration.current,
          task: selectedTask,
          draft,
          configuration: {
            ...configuration,
            source: { ...record(configuration.source), includeSnapshot: false },
          },
          addedTables: addition.addedTables,
        });
        return;
      }
      void persistDraft(configuration, selectedTask, draft);
    } catch (caught) {
      reportSaveFailure(caught);
    }
  }

  function confirmPgTableAddition() {
    if (
      pgAddition === null ||
      pgAddition.context !== saveContext.current ||
      pgAddition.generation !== saveGeneration.current
    ) {
      setPgAddition(null);
      return;
    }
    void persistDraft(pgAddition.configuration, pgAddition.task, pgAddition.draft, true);
  }

  async function runPreflight(taskToCheck = selectedTask) {
    if (taskToCheck === null || checking || dirty) return;
    const request = ++preflightRequest.current;
    const context = preflightContext.current;
    const active = () =>
      request === preflightRequest.current && context === preflightContext.current;
    setChecking(true);
    setError(null);
    setNotice(null);
    try {
      let target = taskToCheck;
      if (target.draft === null) {
        if (
          target.applied == null ||
          target.desiredState !== 'STOPPED' ||
          target.observedState !== 'STOPPED'
        )
          return;
        target = await tasksApi.saveDraft(
          target,
          target.applied.configuration,
          target.applied.artifactId,
        );
        if (request !== preflightRequest.current) return;
        const saved = target;
        setTasks((current) => current.map((task) => (task.id === saved.id ? saved : task)));
        if (!active()) return;
        const next = draftFromTask(saved);
        setDraft(next);
        setBaseline(next);
      }
      const run = await tasksApi.preflight(target);
      if (!active()) return;
      setPreflight(run);
      const refreshed = await tasksApi.list();
      if (!active()) return;
      setTasks(refreshed.tasks);
      const current = refreshed.tasks.find((task) => task.id === taskToCheck.id);
      if (current !== undefined) {
        const next = draftFromTask(current);
        setDraft(next);
        setBaseline(next);
      }
      notify(
        run.state === 'PASSED' ? 'success' : 'warning',
        run.state === 'PASSED'
          ? '预检已通过。任务仍保持停止状态。'
          : '预检未通过，请处理阻断项后重试。',
      );
    } catch (caught) {
      if (active()) reportFailure('运行预检失败', caught);
    } finally {
      if (request === preflightRequest.current) setChecking(false);
    }
  }

  async function preparePgResources() {
    if (!canPreparePg || selectedTask === null || !pgRiskConfirmed) return;
    const request = ++pgRequest.current;
    const context = currentPgContext;
    const active = () => request === pgRequest.current && context === pgContext.current;
    setPreparingPg(true);
    setPgFeedback(null);
    setError(null);
    setNotice(null);
    setPreflight(null);
    let prepared = false;
    try {
      await tasksApi.preparePgResources(selectedTask);
      prepared = true;
      if (!active()) return;
      const refreshed = await tasksApi.list();
      if (!active()) return;
      setTasks(refreshed.tasks);
      const current = refreshed.tasks.find((task) => task.id === selectedTask.id);
      if (current === undefined) throw new Error('无法读取准备后的任务，请刷新任务列表。');
      const run = await tasksApi.preflight(current);
      if (!active()) return;
      const checked = await tasksApi.list();
      if (!active()) return;
      setTasks(checked.tasks);
      const checkedTask = checked.tasks.find((task) => task.id === current.id);
      if (checkedTask !== undefined) {
        const next = draftFromTask(checkedTask);
        setDraft(next);
        setBaseline(next);
      }
      setPreflight(run);
      setPgFeedback({
        context: JSON.stringify([
          editorOpen,
          selectedTaskId,
          checkedTask ? draftFromTask(checkedTask) : draft,
        ]),
        failed: false,
        message:
          run.state === 'PASSED'
            ? 'PostgreSQL 复制资源已核验并绑定，预检通过。任务尚未启动。'
            : 'PostgreSQL 复制资源已核验并绑定，请继续处理预检阻断项。任务尚未启动。',
      });
    } catch (caught) {
      if (active())
        reportFailure(
          prepared ? '复制资源已准备，但刷新或预检失败' : '准备复制资源失败',
          caught,
          'PREPARE_PG',
        );
    } finally {
      if (request === pgRequest.current) {
        setPreparingPg(false);
        setPreparePgOpen(false);
        setPgRiskConfirmed(false);
      }
    }
  }

  async function prepareTargetTopic() {
    if (
      selectedTask === null ||
      !topicPending ||
      topicPreparationBlocked !== null ||
      session.user.role !== 'ADMIN'
    )
      return;
    setStarting(true);
    setError(null);
    setNotice(null);
    try {
      try {
        const operation = await tasksApi.start(selectedTask);
        const refreshed = await tasksApi.list();
        setTasks(refreshed.tasks);
        setPrepareTopicOpen(false);
        reportStartOperation(operation);
        return;
      } catch (caught) {
        if (
          !(caught instanceof PlatformApiError) ||
          caught.code !== 'RUNTIME_TOPIC_PREPARED_RECHECK_REQUIRED'
        )
          throw caught;
      }
      const refreshed = await tasksApi.list();
      setTasks(refreshed.tasks);
      const current = refreshed.tasks.find((task) => task.id === selectedTask.id);
      if (current === undefined) throw new Error('无法读取准备后的任务，请刷新任务列表。');
      setPrepareTopicOpen(false);
      await runPreflight(current);
    } catch (caught) {
      setPrepareTopicOpen(false);
      reportFailure('准备 Topic 失败', caught, 'CREATE_TOPIC');
    } finally {
      setStarting(false);
    }
  }

  function reportStartOperation(operation: RuntimeOperationSummary) {
    if (
      ['BLOCKED_MANUAL', 'BLOCKED_PERMANENT', 'FAILED_RETRYABLE', 'REJECTED'].includes(
        operation.state,
      )
    ) {
      const reason = operation.failureCode?.startsWith('RUNTIME_CGROUP_')
        ? '旧部署模式的资源隔离错误；升级为共享容器模式后，请重新预检并启动。'
        : (operation.failureSummary ?? '请在任务详情中查看失败原因。');
      setError(
        `启动被阻断：${reason}${operation.failureCode ? `（${operation.failureCode}）` : ''}`,
      );
      return;
    }
    notify('info', `启动操作已受理：${operation.state}。可在任务详情中查看进度。`);
  }

  async function startTask() {
    if (
      selectedTask === null ||
      !['PREFLIGHT_PASSED', 'RISK_CONFIRMED'].includes(selectedTask.draft?.state ?? '') ||
      selectedTask.observedState !== 'STOPPED' ||
      dirty ||
      saving ||
      checking ||
      starting ||
      reinitializationPreflight ||
      startPending.current
    ) {
      return;
    }
    startPending.current = true;
    setStarting(true);
    setError(null);
    setNotice(null);
    try {
      const operation = await tasksApi.start(selectedTask);
      const refreshed = await tasksApi.list();
      setTasks(refreshed.tasks);
      reportStartOperation(operation);
    } catch (caught) {
      if (
        caught instanceof PlatformApiError &&
        caught.code === 'RUNTIME_TOPIC_PREPARED_RECHECK_REQUIRED'
      ) {
        try {
          const refreshed = await tasksApi.list();
          setTasks(refreshed.tasks);
          const current = refreshed.tasks.find((task) => task.id === selectedTask.id);
          if (current === undefined)
            throw new Error('无法读取准备后的任务，请刷新任务列表。', { cause: caught });
          await runPreflight(current);
        } catch (refreshFailure) {
          reportFailure('Topic 已准备，但重新预检失败', refreshFailure);
        }
      } else {
        reportFailure('启动任务失败', caught);
      }
    } finally {
      startPending.current = false;
      setStarting(false);
    }
  }

  async function runListRuntimeAction(task: TaskSummary, action: ListRuntimeAction) {
    if (listRuntimePending.current || groupPending.current || pendingTaskRefresh.includes(task.id))
      return;
    if (action === 'START' && (task.desiredState !== 'STOPPED' || task.observedState !== 'STOPPED'))
      return;
    if (action === 'STOP' && !['STARTING', 'RUNNING'].includes(task.observedState)) return;
    if (
      action === 'RESTART' &&
      (task.desiredState !== 'RUNNING' ||
        task.observedState !== 'RUNNING' ||
        !restartConfigurationMatches(task))
    )
      return;
    listRuntimePending.current = true;
    invalidateListLoad();
    const generation = listRuntimeGeneration.current;
    const active = () => generation === listRuntimeGeneration.current;
    let restartDraft = task.draft;
    const assertConfirmedRestart = (current: TaskSummary) => {
      if (action !== 'RESTART') return;
      if (
        current.applied?.id !== task.applied?.id ||
        current.currentAppliedRevisionId !== task.currentAppliedRevisionId ||
        current.draft?.id !== restartDraft?.id ||
        current.draft?.draftVersion !== restartDraft?.draftVersion ||
        !restartConfigurationMatches(current)
      )
        throw new Error('任务配置在确认后已变化，未继续重启，请重新确认。');
    };
    setListRuntimeBusy(true);
    setError(null);
    setNotice(null);
    try {
      let current = task;
      if (action !== 'START') {
        const stopped = await tasksApi.stop(current);
        if (!active()) return;
        if (
          ['FAILED_RETRYABLE', 'BLOCKED_MANUAL', 'BLOCKED_PERMANENT', 'REJECTED'].includes(
            stopped.state,
          )
        ) {
          setError(
            `停止未完成：${stopped.failureSummary ?? stopped.failureCode ?? operationName(stopped.state)}`,
          );
          return;
        }
        for (let attempt = 0; attempt < 30; attempt += 1) {
          const refreshed = await tasksApi.list();
          if (!active()) return;
          setTasks(refreshed.tasks);
          const found = refreshed.tasks.find((item) => item.id === task.id);
          if (found === undefined) throw new Error('任务已不存在，请刷新列表。');
          current = found;
          assertConfirmedRestart(current);
          if (current.desiredState === 'STOPPED' && current.observedState === 'STOPPED') break;
          if (action === 'STOP') break;
          await new Promise((resolve) => setTimeout(resolve, 1_000));
          if (!active()) return;
        }
        if (action === 'STOP') {
          notify('info', '停止操作已受理，请刷新列表查看状态。');
          return;
        }
        if (current.desiredState !== 'STOPPED' || current.observedState !== 'STOPPED') {
          notify('warning', '任务尚未停止，未继续启动。请刷新列表确认状态后操作。');
          return;
        }
        assertConfirmedRestart(current);
      }
      if (current.draft === null) {
        if (current.applied == null) throw new Error('请先编辑并保存任务配置。');
        current = await tasksApi.saveDraft(
          current,
          current.applied.configuration,
          current.applied.artifactId,
        );
        if (!active()) return;
        restartDraft = current.draft;
      }
      assertConfirmedRestart(current);
      const checked = await tasksApi.preflight(current);
      if (!active()) return;
      const refreshed = await tasksApi.list();
      if (!active()) return;
      setTasks(refreshed.tasks);
      const found = refreshed.tasks.find((item) => item.id === task.id);
      if (checked.state !== 'PASSED') {
        notify('warning', '预检未通过，任务未启动。请编辑任务查看并处理阻断项。');
        return;
      }
      if (
        found === undefined ||
        found.draft?.id !== checked.revisionId ||
        found.draft.draftVersion !== checked.result.taskRevision.draftVersion ||
        found.rowVersion !== checked.result.taskRevision.taskRowVersion
      )
        throw new Error('任务配置已变化，请重新预检。');
      assertConfirmedRestart(found);
      const operation = await tasksApi.start(found);
      if (!active()) return;
      reportStartOperation(operation);
      const latest = await tasksApi.list();
      if (active()) setTasks(latest.tasks);
    } catch (caught) {
      if (active())
        reportFailure(
          action === 'START' ? '启动任务失败' : action === 'STOP' ? '停止任务失败' : '重新启动失败',
          caught,
        );
    } finally {
      if (active()) {
        listRuntimePending.current = false;
        setListRuntimeBusy(false);
        setListRuntimePrompt(null);
      }
    }
  }

  function openMysqlReinitialization(task: TaskSummary) {
    if (
      task.taskType !== 'MYSQL_KAFKA' ||
      task.desiredState !== 'STOPPED' ||
      task.observedState !== 'STOPPED'
    )
      return;
    setDetailTask(null);
    if (task.draft === null) {
      selectTask(task);
      setNotice('请先保存当前已应用配置为草稿，再执行重新全量。');
      return;
    }
    if (record(task.draft.configuration.source).includeSnapshot !== true) {
      selectTask(task);
      setNotice('请先开启“首次运行包含初始快照”并保存草稿，再执行重新全量。');
      return;
    }
    setReinitializeTarget(task);
  }

  async function reinitializeMysqlSnapshot() {
    const target = reinitializeTarget;
    if (
      target === null ||
      target.draft === null ||
      startPending.current ||
      saving ||
      checking ||
      starting
    )
      return;
    startPending.current = true;
    invalidateListLoad();
    const request = ++reinitializeRequest.current;
    setStarting(true);
    setError(null);
    setNotice(null);
    try {
      const run = await tasksApi.preflight(target, { reinitializeMysqlSnapshot: true });
      if (request !== reinitializeRequest.current) return;
      const refreshed = await tasksApi.list();
      if (request !== reinitializeRequest.current) return;
      setTasks(refreshed.tasks);
      const current = refreshed.tasks.find((task) => task.id === target.id);
      if (current === undefined) throw new Error('无法读取任务，请刷新任务列表。');
      const next = draftFromTask(current);
      setSelectedTaskId(current.id);
      setDraft(next);
      setBaseline(next);
      setPreflight(run);
      setStep(3);
      setEditorOpen(true);
      setReinitializeTarget(null);
      if (run.state !== 'PASSED') {
        setError('重新全量预检未通过，任务未启动，原检查点未重置。');
        return;
      }
      if (
        current.draft?.id !== run.revisionId ||
        current.draft.draftVersion !== run.result.taskRevision.draftVersion ||
        !['PREFLIGHT_PASSED', 'RISK_CONFIRMED'].includes(current.draft.state) ||
        current.desiredState !== 'STOPPED' ||
        current.observedState !== 'STOPPED'
      ) {
        throw new Error('任务配置或状态已变化，请重新确认后再执行重新全量。');
      }
      const operation = await tasksApi.start(current, { reinitializeMysqlSnapshot: true });
      if (request !== reinitializeRequest.current) return;
      reportStartOperation(operation);
      try {
        const latest = await tasksApi.list();
        if (request === reinitializeRequest.current) setTasks(latest.tasks);
      } catch (caught) {
        if (request === reinitializeRequest.current)
          reportFailure('重新全量操作已受理，但刷新任务列表失败', caught);
      }
    } catch (caught) {
      if (request === reinitializeRequest.current) {
        setReinitializeTarget(null);
        reportFailure('重新全量失败', caught);
      }
    } finally {
      startPending.current = false;
      if (request === reinitializeRequest.current) setStarting(false);
    }
  }

  async function deleteTask() {
    if (
      deleteTarget === null ||
      deleteTarget.desiredState !== 'STOPPED' ||
      !deletableObservedStates.has(deleteTarget.observedState) ||
      deleting
    ) {
      return;
    }
    invalidateListLoad();
    setDeleting(true);
    setError(null);
    setNotice(null);
    try {
      const deletedTaskId = deleteTarget.id;
      await tasksApi.remove(deleteTarget);

      const applyRemainingTasks = (remainingTasks: readonly TaskSummary[]) => {
        setTasks(remainingTasks);
        const next = newDraft();
        setEditorOpen(false);
        setSelectedTaskId(null);
        setDraft(next);
        setBaseline(next);
      };

      applyRemainingTasks(tasks.filter((task) => task.id !== deletedTaskId));
      invalidatePreflight();
      clearDeletePrompt();
      setNotice('任务已删除。历史修订、操作记录和审计记录仍由系统保留。');

      try {
        const refreshed = await tasksApi.list();
        applyRemainingTasks(refreshed.tasks);
      } catch (caught) {
        if (
          caught instanceof PlatformApiError &&
          (caught.status === 401 || caught.code === 'SESSION_INVALID')
        ) {
          onSessionInvalidated();
        } else {
          const detail =
            caught instanceof PlatformApiError
              ? caught.code
              : caught instanceof Error
                ? caught.message
                : null;
          setError(
            detail === null
              ? '任务已删除，但刷新任务列表失败。'
              : `任务已删除，但刷新任务列表失败：${detail}`,
          );
        }
      }
    } catch (caught) {
      reportFailure('删除任务失败', caught, 'DELETE_TASK');
    } finally {
      setDeleting(false);
    }
  }

  return (
    <>
      {holder}
      {loading ? (
        <Card>
          <CardContent className="flex items-center gap-3 py-12">
            <Loader2 className="size-5 animate-spin" />
            <p role="status">正在读取任务与数据源…</p>
          </CardContent>
        </Card>
      ) : (
        <div className={editorOpen ? 'task-management is-editing' : 'task-management'}>
          {!editorOpen && (
            <div className="task-list-page">
              <div className="task-list-toolbar">
                <Input
                  className="max-w-sm"
                  aria-label="筛选任务"
                  placeholder="按任务名称筛选…"
                  value={query}
                  onChange={(event) => {
                    setQuery(event.target.value);
                    setListPage((page) => ({ ...page, current: 1 }));
                  }}
                />
                <span className="text-sm text-muted-foreground">{visibleTasks.length} 个任务</span>
                <Checkbox
                  aria-label="列表自动刷新"
                  checked={autoRefreshList}
                  onChange={(event) => setAutoRefreshList(event.target.checked)}
                >
                  自动刷新
                </Checkbox>
                <Select
                  aria-label="列表刷新间隔"
                  value={listRefreshInterval}
                  onChange={setListRefreshInterval}
                  options={[5, 15, 30, 60].map((seconds) => ({
                    value: seconds * 1000,
                    label: `${seconds} 秒`,
                  }))}
                  style={{ width: 100 }}
                />
                <Button
                  type="button"
                  variant="outline"
                  disabled={refreshingList || progress.refreshing || listRuntimeBusy || groupBusy}
                  onClick={() => void refreshList()}
                >
                  <RefreshCw
                    className={refreshingList || progress.refreshing ? 'animate-spin' : ''}
                  />
                  刷新任务列表
                </Button>
                <Button
                  className="ml-auto"
                  type="button"
                  aria-label="新建任务"
                  onClick={beginCreate}
                >
                  <Plus />
                  新建任务
                </Button>
              </div>
              <div className="task-list-layout">
                <div className="task-list-groups rounded-xl border bg-card p-2">
                  <TaskGroupsPanel
                    groups={groups}
                    tasks={tasks}
                    selectedGroupId={selectedGroupId}
                    onSelect={(id) => {
                      setSelectedGroupId(id);
                      setListPage((page) => ({ ...page, current: 1 }));
                    }}
                    onCreate={() => openGroupPrompt('LIST')}
                    onAssign={(taskId, groupId) => void assignGroup(taskId, groupId)}
                    onRename={(group) => {
                      setRenamingGroup(group);
                      setGroupName(group.name);
                      setGroupPrompt('RENAME');
                    }}
                    onDelete={(group) =>
                      setDeletingGroup({
                        group,
                        occupied: tasks.some((task) => task.groupId === group.id),
                      })
                    }
                    onReorder={(ids) => void reorderGroups(ids)}
                    busy={
                      groupBusy || groupsLoading || groupsError || listRuntimeBusy || refreshingList
                    }
                  />
                  {groupsError && (
                    <p role="status" className="p-2 text-xs text-muted-foreground">
                      任务组读取失败。
                      <Button variant="link" size="sm" onClick={() => void loadGroups()}>
                        重试
                      </Button>
                    </p>
                  )}
                </div>
                <section className="task-list-results" aria-label="任务列表区域">
                  {progress.error && (
                    <p role="status" className="text-sm">
                      消费进度刷新失败，保留上次已采集结果，不影响任务操作。
                    </p>
                  )}
                  <div
                    className="task-card-viewport"
                    ref={listViewport}
                    tabIndex={0}
                    role="region"
                    aria-label="任务卡片"
                  >
                    <ul className="task-card-grid" aria-label="任务列表">
                      {pagedTasks.map((task) => {
                        const pending = pendingTaskRefresh.includes(task.id);
                        const recoverable =
                          session.user.role === 'ADMIN' &&
                          task.observedState === 'BLOCKED' &&
                          ['AUTO_ELIGIBLE', 'MANUAL_REQUIRED', 'CAPACITY_BLOCKED'].includes(
                            task.recoveryDisposition,
                          );
                        return (
                          <TaskCard
                            key={task.id}
                            task={task}
                            typeLabel={taskTypeLabel(task.taskType)}
                            sourceName={
                              dataSources.find((source) => source.id === task.sourceDataSourceId)
                                ?.name ?? '来源未载入'
                            }
                            targetName={
                              dataSources.find((source) => source.id === task.targetDataSourceId)
                                ?.name ?? '目标未载入'
                            }
                            groupName={
                              selectedGroupId === 'ALL' && task.groupId
                                ? groups.find((group) => group.id === task.groupId)?.name
                                : undefined
                            }
                            progress={progress.items.find((item) => item.taskId === task.id)}
                            pending={pending}
                            dragDisabled={groupBusy || groupsError || listRuntimeBusy || pending}
                            status={
                              <Badge
                                variant="outline"
                                className={observedStateBadgeClassNames[task.observedState]}
                                title={task.observedState}
                              >
                                {pending ? '已停止，待刷新' : operationName(task.observedState)}
                              </Badge>
                            }
                            onDetails={() => {
                              invalidateListLoad();
                              setDetailTask(task);
                            }}
                          >
                            {recoverable ? (
                              <Button
                                type="button"
                                size="sm"
                                variant="ghost"
                                aria-label={`${task.name} 恢复任务`}
                                className="text-emerald-700 hover:bg-emerald-50 dark:text-emerald-400 dark:hover:bg-emerald-950"
                                disabled={
                                  pending ||
                                  (task.desiredState !== 'RUNNING' &&
                                    task.recoveryDisposition !== 'MANUAL_REQUIRED')
                                }
                                onClick={() => {
                                  setRecoveryAction('RETRY');
                                  setDiscardConfirmed(false);
                                  setDiscardTarget(task);
                                }}
                              >
                                <RefreshCw aria-hidden="true" />
                                恢复
                              </Button>
                            ) : !['STARTING', 'RUNNING', 'STOPPING'].includes(
                                task.observedState,
                              ) ? (
                              <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                aria-label={`${task.name} 开始`}
                                className="text-emerald-700 hover:bg-emerald-50 dark:text-emerald-400 dark:hover:bg-emerald-950"
                                title={
                                  task.observedState === 'STOPPED' &&
                                  task.desiredState === 'STOPPED'
                                    ? '预检通过后启动任务'
                                    : '任务停止后才可开始'
                                }
                                disabled={
                                  listRuntimeBusy ||
                                  groupBusy ||
                                  pending ||
                                  task.observedState !== 'STOPPED' ||
                                  task.desiredState !== 'STOPPED' ||
                                  (task.draft === null && task.applied == null)
                                }
                                onClick={() => void runListRuntimeAction(task, 'START')}
                              >
                                <Play aria-hidden="true" />
                                开始
                              </Button>
                            ) : (
                              <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                aria-label={`${task.name} 停止`}
                                className="text-amber-700 hover:bg-amber-50 dark:text-amber-400 dark:hover:bg-amber-950"
                                title="停止任务；仅启动中或运行中的任务可停止"
                                disabled={
                                  listRuntimeBusy ||
                                  groupBusy ||
                                  pending ||
                                  !['STARTING', 'RUNNING'].includes(task.observedState)
                                }
                                onClick={() => setListRuntimePrompt({ task, action: 'STOP' })}
                              >
                                <Square aria-hidden="true" />
                                停止
                              </Button>
                            )}
                            <DropdownMenu>
                              <DropdownMenuTrigger asChild>
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="icon"
                                  className="size-8"
                                  aria-label={`${task.name} 更多操作`}
                                >
                                  <MoreHorizontal aria-hidden="true" />
                                </Button>
                              </DropdownMenuTrigger>
                              <DropdownMenuContent align="end">
                                {task.observedState === 'RUNNING' && (
                                  <DropdownMenuItem
                                    aria-label={`${task.name} 重新启动`}
                                    className="text-violet-700 dark:text-violet-400"
                                    title={
                                      !restartConfigurationMatches(task)
                                        ? '请先处理与已应用配置不同的草稿，再重新启动'
                                        : '按已应用配置受控停止并重启，不重新全量'
                                    }
                                    disabled={
                                      listRuntimeBusy ||
                                      groupBusy ||
                                      pending ||
                                      task.desiredState !== 'RUNNING' ||
                                      !restartConfigurationMatches(task)
                                    }
                                    onSelect={() =>
                                      setListRuntimePrompt({ task, action: 'RESTART' })
                                    }
                                  >
                                    <RefreshCw aria-hidden="true" />
                                    重新启动
                                  </DropdownMenuItem>
                                )}
                                <DropdownMenuItem
                                  aria-label={`${task.name} 复制`}
                                  disabled={pending}
                                  onSelect={() => beginCopyFrom(task)}
                                >
                                  <Copy aria-hidden="true" />
                                  复制任务
                                </DropdownMenuItem>
                                {task.taskType === 'MYSQL_KAFKA' &&
                                  task.desiredState === 'STOPPED' &&
                                  task.observedState === 'STOPPED' && (
                                    <DropdownMenuItem
                                      aria-label={`${task.name} 重新全量`}
                                      disabled={starting || pending}
                                      onSelect={() => openMysqlReinitialization(task)}
                                    >
                                      <RefreshCw aria-hidden="true" />
                                      重新全量
                                    </DropdownMenuItem>
                                  )}
                                {session.user.role === 'ADMIN' &&
                                  task.observedState === 'BLOCKED' && (
                                    <DropdownMenuItem
                                      aria-label={`${task.name} 放弃恢复`}
                                      disabled={pending}
                                      onSelect={() => {
                                        setRecoveryAction('DISCARD');
                                        setError(null);
                                        setNotice(null);
                                        setDiscardConfirmed(false);
                                        setDiscardTarget(task);
                                      }}
                                    >
                                      <Square aria-hidden="true" />
                                      放弃恢复
                                    </DropdownMenuItem>
                                  )}
                                <DropdownMenuSeparator />
                                <DropdownMenuItem
                                  variant="destructive"
                                  aria-label={`${task.name} 删除`}
                                  title={
                                    task.desiredState === 'STOPPED' &&
                                    deletableObservedStates.has(task.observedState)
                                      ? '删除任务'
                                      : '请先停止任务后再删除'
                                  }
                                  disabled={
                                    pending ||
                                    task.desiredState !== 'STOPPED' ||
                                    !deletableObservedStates.has(task.observedState)
                                  }
                                  onSelect={() => openDeletePrompt(task)}
                                >
                                  <Trash2 aria-hidden="true" />
                                  删除任务
                                </DropdownMenuItem>
                              </DropdownMenuContent>
                            </DropdownMenu>
                          </TaskCard>
                        );
                      })}
                    </ul>
                    {visibleTasks.length === 0 && (
                      <div className="py-16 text-center text-sm text-muted-foreground">
                        {tasks.length === 0 ? '暂无任务' : '没有匹配的任务'}
                      </div>
                    )}
                  </div>
                  <div className="task-list-pagination">
                    <Pagination
                      size="small"
                      total={visibleTasks.length}
                      current={currentListPage}
                      pageSize={listPage.pageSize}
                      pageSizeOptions={[10, 20, 50]}
                      showSizeChanger={{ 'aria-label': '任务每页条数' }}
                      onChange={(current, pageSize) =>
                        setListPage({
                          current: pageSize === listPage.pageSize ? current : 1,
                          pageSize,
                        })
                      }
                    />
                  </div>
                </section>
              </div>
            </div>
          )}

          {editorOpen && (
            <section className="task-editor" aria-label="任务配置">
              <div className="task-editor-heading flex flex-wrap items-center gap-4">
                <Button
                  type="button"
                  variant="outline"
                  aria-label="返回任务列表"
                  onClick={closeEditor}
                >
                  <ArrowLeft />
                  返回任务列表
                </Button>
                <h2 className="text-xl font-semibold">
                  {selectedTask === null ? '新建同步任务' : `编辑 ${selectedTask.name}`}
                </h2>
              </div>
              <div
                className="task-editor-flow grid gap-3 rounded-lg border bg-muted/20 px-4 py-2 sm:grid-cols-[1fr_auto_1fr]"
                aria-label="数据流摘要"
              >
                <div className="min-w-0">
                  <p className="text-xs font-medium text-muted-foreground">来源</p>
                  <p className="truncate font-medium" title={sourceDataSource?.name}>
                    {sourceDataSource?.name ?? '未选择来源'}
                  </p>
                  <p
                    className="truncate text-xs text-muted-foreground"
                    title={draft.mappings
                      .map((row) => mappingSourceLabel(row, draft.taskType))
                      .join('、')}
                  >
                    {draft.mappings
                      .map((row) => mappingSourceLabel(row, draft.taskType))
                      .filter(Boolean)
                      .join('、') || '未配置来源映射'}
                  </p>
                </div>
                <span className="self-center text-xl text-muted-foreground" aria-hidden="true">
                  →
                </span>
                <div className="min-w-0">
                  <p className="text-xs font-medium text-muted-foreground">目标 Kafka</p>
                  <p className="truncate font-medium" title={targetDataSource?.name}>
                    {targetDataSource?.name ?? '未选择目标'}
                  </p>
                  <p
                    className="truncate text-xs text-muted-foreground"
                    title={mappingTargets(draft.mappings)
                      .map((item) => item.topic)
                      .join('、')}
                  >
                    {mappingTargets(draft.mappings)
                      .map((item) => item.topic)
                      .join('、') || '未填写目标 Topic'}
                  </p>
                </div>
              </div>
              <nav className="grid grid-cols-2 gap-2 sm:grid-cols-4" aria-label="任务配置步骤">
                {['来源 Source', '目标 Sink', '运行配置', '确认与预检'].map((label, index) => (
                  <Button
                    key={label}
                    variant={step === index ? 'default' : 'outline'}
                    aria-current={step === index ? 'step' : undefined}
                    onClick={() => setStep(index)}
                  >
                    {index + 1}. {label}
                  </Button>
                ))}
              </nav>
              <div className="task-editor-body">
                {step === 0 && (
                  <div className="task-editor-source">
                    <div
                      role="group"
                      aria-label={
                        draft.taskType === 'PG_KAFKA' ? '来源与 PostgreSQL 复制资源' : '来源配置'
                      }
                      className="grid items-start gap-4 lg:grid-cols-2"
                    >
                      <Card className="gap-4 py-4">
                        <CardHeader className="flex flex-row items-center justify-between px-4">
                          <CardTitle>来源 Source</CardTitle>
                          {selectedTask !== null && (
                            <Button variant="outline" onClick={beginCopy}>
                              <Copy />
                              复制任务
                            </Button>
                          )}
                        </CardHeader>
                        <CardContent className="grid gap-4 px-4 md:grid-cols-2">
                          <Field label="任务名称">
                            <Input
                              aria-label="任务名称"
                              disabled={selectedTask !== null}
                              maxLength={128}
                              value={draft.name}
                              onChange={(event) => update('name', event.target.value)}
                            />
                          </Field>
                          <Field label="任务类型">
                            <select
                              className={selectStyle}
                              aria-label="任务类型"
                              disabled={selectedTask !== null}
                              value={draft.taskType}
                              onChange={(event) => {
                                const next = {
                                  ...newDraft(event.target.value as TaskType),
                                  name: draft.name,
                                };
                                setDraft(next);
                                invalidatePreflight();
                                setNotice(null);
                              }}
                            >
                              <option value="PG_KAFKA">PostgreSQL → Kafka</option>
                              <option value="MYSQL_KAFKA">MySQL → Kafka</option>
                              <option value="KAFKA_KAFKA">Kafka → Kafka Raw</option>
                            </select>
                          </Field>
                          <Field label="源数据源">
                            <select
                              className={selectStyle}
                              aria-label="源数据源"
                              disabled={selectedTask !== null}
                              value={draft.sourceDataSourceId}
                              onChange={(event) => {
                                const nextSource = dataSources.find(
                                  (source) => source.id === event.target.value,
                                );
                                const sourceSchema =
                                  nextSource?.configuration.type === 'MYSQL'
                                    ? nextSource.configuration.database
                                    : '';
                                setDraft((current) => ({
                                  ...current,
                                  sourceDataSourceId: event.target.value,
                                  mappings: [{ ...emptyTaskMapping(), sourceSchema }],
                                }));
                                invalidatePreflight();
                              }}
                            >
                              <option value="">请选择</option>
                              {sourceOptions.map((source) => (
                                <option key={source.id} value={source.id}>
                                  {source.name}
                                </option>
                              ))}
                            </select>
                          </Field>

                          <div className="space-y-1">
                            <TaskGroupSelect
                              groups={groups}
                              value={
                                selectedTask === null
                                  ? draft.groupId
                                  : (selectedTask.groupId ?? null)
                              }
                              onChange={(groupId) =>
                                selectedTask === null
                                  ? update('groupId', groupId)
                                  : void assignGroup(selectedTask.id, groupId)
                              }
                              onCreate={() => openGroupPrompt('EDITOR')}
                              disabled={groupBusy || groupsLoading || groupsError || saving}
                            />
                            {selectedTask !== null && (
                              <p className="text-xs text-muted-foreground">
                                分组修改即时保存，不影响同步配置。
                              </p>
                            )}
                            {groupsError && (
                              <p className="text-xs text-destructive">
                                任务组读取失败，请返回列表重试。
                              </p>
                            )}
                          </div>
                        </CardContent>
                      </Card>
                      {draft.taskType === 'PG_KAFKA' ? (
                        <Card className="gap-4 py-4">
                          <CardHeader className="flex flex-row items-center gap-2 px-4">
                            <CardTitle>PostgreSQL 复制资源</CardTitle>
                            <Help
                              label="PostgreSQL 复制资源说明"
                              text="此处管理逻辑复制槽和发布，不是 CPU 或内存。保存后由管理员核验或准备；增量加表仅扩展原 Publication，保留复制槽和位点，准备不会启动同步。"
                            />
                          </CardHeader>
                          <CardContent className="space-y-4 px-4">
                            <fieldset>
                              <legend className="mb-3 flex items-center gap-2 text-sm font-medium">
                                复制槽与发布的管理方式
                                <Help
                                  label="当前管理方式说明"
                                  text={
                                    draft.resourceMode === 'SELF_MANAGED'
                                      ? '使用管理员预先创建的 Slot 和 Publication，平台不负责创建。'
                                      : draft.resourceMode === 'PLATFORM_MANAGED'
                                        ? '由管理员确认后，平台创建缺失的复制槽与发布，并核验、绑定已有资源。'
                                        : '由 ape-dts 管理复制资源；不支持初始快照，也不使用平台准备入口。'
                                  }
                                />
                              </legend>
                              <div className="flex flex-wrap gap-5">
                                {(
                                  [
                                    'SELF_MANAGED',
                                    'PLATFORM_MANAGED',
                                    ...(session.user.role === 'ADMIN' ? ['APE_DTS_MANAGED'] : []),
                                  ] as ResourceMode[]
                                ).map((mode) => (
                                  <label key={mode} className="flex items-center gap-2 text-sm">
                                    <input
                                      type="radio"
                                      name="resourceMode"
                                      value={mode}
                                      checked={draft.resourceMode === mode}
                                      onChange={() => {
                                        if (mode === 'APE_DTS_MANAGED' && draft.includeSnapshot) {
                                          setError('关闭初始快照后，才能选择 ape-dts 管理资源。');
                                          return;
                                        }
                                        update('resourceMode', mode);
                                      }}
                                    />
                                    {mode === 'SELF_MANAGED'
                                      ? '使用已有资源'
                                      : mode === 'PLATFORM_MANAGED'
                                        ? '平台管理'
                                        : '引擎管理（高级）'}
                                  </label>
                                ))}
                              </div>
                            </fieldset>
                            <div className="grid gap-4 md:grid-cols-2">
                              <Field label="Slot 名称">
                                <Input
                                  aria-label="Slot 名称"
                                  value={draft.slotName}
                                  onChange={(event) => update('slotName', event.target.value)}
                                />
                              </Field>
                              <Field label="Publication 名称">
                                <Input
                                  aria-label="Publication 名称"
                                  value={draft.publicationName}
                                  onChange={(event) =>
                                    update('publicationName', event.target.value)
                                  }
                                />
                              </Field>
                            </div>
                            <label className="flex items-center gap-2 text-sm">
                              <input
                                type="checkbox"
                                checked={draft.includeSnapshot}
                                disabled={draft.resourceMode === 'APE_DTS_MANAGED'}
                                onChange={(event) =>
                                  update('includeSnapshot', event.target.checked)
                                }
                              />
                              首次运行包含初始快照
                            </label>
                            {session.user.role === 'ADMIN' &&
                              draft.resourceMode !== 'APE_DTS_MANAGED' && (
                                <div className="flex flex-wrap items-center gap-3">
                                  <Button
                                    variant="outline"
                                    disabled={!canPreparePg}
                                    onClick={() => {
                                      setError(null);
                                      setNotice(null);
                                      setPgFeedback(null);
                                      setPgRiskConfirmed(false);
                                      setPreparePgOpen(true);
                                    }}
                                  >
                                    {draft.resourceMode === 'SELF_MANAGED'
                                      ? '核验并绑定复制资源'
                                      : '准备复制资源'}
                                  </Button>
                                  {preparingPg ? (
                                    <p role="status" className="text-xs text-muted-foreground">
                                      正在准备复制资源并重新预检…
                                    </p>
                                  ) : pgFeedback?.context === currentPgContext ? (
                                    <p
                                      role={pgFeedback.failed ? 'alert' : 'status'}
                                      className={
                                        pgFeedback.failed
                                          ? 'text-xs text-destructive'
                                          : 'text-xs text-muted-foreground'
                                      }
                                    >
                                      {pgFeedback.message}
                                    </p>
                                  ) : pgPreparationGuidance !== null ? (
                                    <p className="text-xs text-muted-foreground">
                                      {pgPreparationGuidance}
                                    </p>
                                  ) : null}
                                </div>
                              )}
                          </CardContent>
                        </Card>
                      ) : draft.taskType === 'MYSQL_KAFKA' ? (
                        <Card className="gap-4 py-4">
                          <CardHeader className="flex flex-row items-center gap-2 px-4">
                            <CardTitle>MySQL 采集</CardTitle>
                            <Help
                              label="MySQL 采集说明"
                              text="初始快照先读取已有数据，再从已记录的 Binlog 起点采集增量；关闭时从首次启动记录的位点开始，不补采此前的数据。重新启动使用已确认检查点，不跳到最新位置。"
                            />
                          </CardHeader>
                          <CardContent className="space-y-4 px-4">
                            <label className="flex items-center gap-2 text-sm">
                              <input
                                type="checkbox"
                                checked={draft.includeSnapshot}
                                onChange={(event) =>
                                  update('includeSnapshot', event.target.checked)
                                }
                              />
                              首次运行包含初始快照
                            </label>
                            <p className="text-xs text-muted-foreground">
                              来源数据库：{sourceDatabase || '请选择 MySQL 数据源'}
                            </p>
                          </CardContent>
                        </Card>
                      ) : (
                        <div className="flex items-center gap-2 rounded-lg border bg-muted/30 p-4 text-sm">
                          Kafka Group ID 由系统生成
                          <Help
                            label="Group ID 说明"
                            text="Group ID 按任务运行绑定，用户无需填写，也不会复用页面占位值。"
                          />
                        </div>
                      )}
                    </div>
                    {(sourceOptions.length === 0 || kafkaSources.length === 0) && (
                      <div className="flex items-center justify-between rounded-md border p-3 text-sm">
                        <span>缺少当前任务类型所需的数据源</span>
                        <Button variant="outline" onClick={onOpenDataSources}>
                          前往数据源管理
                        </Button>
                      </div>
                    )}
                  </div>
                )}
                {step === 1 && (
                  <Card className="task-editor-sink">
                    <CardHeader className="flex flex-row items-center justify-between">
                      <CardTitle>来源与目标映射</CardTitle>
                      {draft.taskType !== 'KAFKA_KAFKA' && (
                        <div className="flex items-center gap-2 text-sm font-medium">
                          消息编码
                          <Help
                            label="消息编码说明"
                            text="Avro 为二进制消息，JSON 为文本消息。CloudCanal 模板需所选制品支持 message_format 与 json_template；字段定义仅影响 Avro。修改格式只影响下次启动后写入的新消息，已有 Topic 消息不会转换。JSON 建议使用测试 Topic 核对实际输出。"
                          />
                        </div>
                      )}
                    </CardHeader>
                    <CardContent className="task-editor-sink-grid">
                      <div className="task-editor-sink-controls">
                        <Field label="目标 Kafka">
                          <select
                            className={selectStyle}
                            aria-label="目标 Kafka"
                            disabled={selectedTask !== null}
                            value={draft.targetDataSourceId}
                            onChange={(event) => {
                              setDraft((current) => ({
                                ...current,
                                targetDataSourceId: event.target.value,
                                mappings: current.mappings.map((row) => ({
                                  ...row,
                                  targets: [],
                                })),
                              }));
                              invalidatePreflight();
                            }}
                          >
                            <option value="">请选择</option>
                            {kafkaSources.map((source) => (
                              <option key={source.id} value={source.id}>
                                {source.name}
                              </option>
                            ))}
                          </select>
                        </Field>
                        {draft.taskType !== 'KAFKA_KAFKA' && (
                          <>
                            <Field label="消息格式">
                              <select
                                className={selectStyle}
                                aria-label="消息格式"
                                value={draft.messageFormat}
                                onChange={(event) => {
                                  const value = event.target.value;
                                  if (value !== '' && value !== 'avro' && value !== 'json') return;
                                  setDraft((current) => ({
                                    ...current,
                                    messageFormat: value,
                                    jsonTemplate:
                                      value === 'json' ? current.jsonTemplate || 'cloudcanal' : '',
                                  }));
                                  invalidatePreflight();
                                }}
                              >
                                <option value="">制品默认（Avro）</option>
                                <option value="avro">Avro（二进制）</option>
                                <option value="json">JSON（文本）</option>
                              </select>
                            </Field>
                            {draft.messageFormat === 'json' && (
                              <Field label="JSON 模板">
                                <select
                                  className={selectStyle}
                                  aria-label="JSON 模板"
                                  value={draft.jsonTemplate}
                                  onChange={(event) => {
                                    const value = event.target.value;
                                    if (
                                      value === '' ||
                                      value === 'standard' ||
                                      value === 'cloudcanal'
                                    )
                                      update('jsonTemplate', value);
                                  }}
                                >
                                  <option value="">制品默认（标准 JSON）</option>
                                  <option value="standard">标准 JSON</option>
                                  <option value="cloudcanal">CloudCanal</option>
                                </select>
                              </Field>
                            )}
                            <Field label="字段定义（Avro）">
                              <select
                                className={selectStyle}
                                aria-label="字段定义（Avro）"
                                value={
                                  draft.withFieldDefs === null ? '' : String(draft.withFieldDefs)
                                }
                                onChange={(event) =>
                                  update(
                                    'withFieldDefs',
                                    event.target.value === ''
                                      ? null
                                      : event.target.value === 'true',
                                  )
                                }
                              >
                                <option value="">制品默认（包含）</option>
                                <option value="true">包含字段定义</option>
                                <option value="false">不包含字段定义</option>
                              </select>
                            </Field>
                          </>
                        )}
                      </div>
                      <div className="task-editor-sink-panels">
                        <div className="task-editor-mappings">
                          <SourceSinkMappings
                            rows={draft.mappings}
                            taskType={draft.taskType}
                            source={sourceDataSource}
                            target={targetDataSource}
                            onChange={(rows) => update('mappings', rows)}
                            canCreate={session.user.role === 'ADMIN'}
                            creating={creatingTopic}
                            onCreateTopic={prepareTopicCreation}
                            refreshKey={topicRefresh}
                          />
                        </div>
                        <MessageEncodingPreview
                          title="消息示例"
                          taskType={draft.taskType}
                          database={sourceDatabase}
                          mappings={draft.mappings}
                          messageFormat={draft.messageFormat}
                          jsonTemplate={draft.jsonTemplate}
                          withFieldDefs={draft.withFieldDefs}
                        />
                      </div>
                    </CardContent>
                  </Card>
                )}
                {step === 2 && (
                  <div className="task-editor-runtime grid items-start gap-4 lg:grid-cols-2">
                    <Card>
                      <CardHeader>
                        <CardTitle>运行制品</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <Field
                          label="运行制品"
                          help="选择已上传的 ape-dts 制品。可先保存未绑定的草稿，启动前需要选择制品；实际兼容性在运行时验证。"
                        >
                          <select
                            className={selectStyle}
                            aria-label="运行制品"
                            value={draft.artifactId ?? ''}
                            onChange={(event) => update('artifactId', event.target.value || null)}
                          >
                            <option value="">
                              {artifacts.length === 0
                                ? '暂无可选制品，请联系管理员上传'
                                : '未绑定制品'}
                            </option>
                            {artifacts.map((artifact) => (
                              <option
                                key={artifact.id}
                                value={artifact.id}
                                disabled={artifact.state === 'QUARANTINED'}
                              >
                                {artifact.versionIdentifier}
                                {artifact.state === 'QUARANTINED' ? '（已隔离）' : ''}
                              </option>
                            ))}
                            {draft.artifactId &&
                              !artifacts.some((artifact) => artifact.id === draft.artifactId) && (
                                <option value={draft.artifactId} disabled>
                                  当前绑定：{draft.artifactId}
                                </option>
                              )}
                          </select>
                          {artifactError && (
                            <div className="flex items-center gap-3 text-sm" role="alert">
                              {artifactError}
                              <Button
                                type="button"
                                size="sm"
                                variant="outline"
                                onClick={() => setArtifactReload((value) => value + 1)}
                              >
                                重试
                              </Button>
                            </div>
                          )}
                        </Field>
                      </CardContent>
                    </Card>
                    <Card>
                      <CardHeader className="flex-row items-center gap-2">
                        <CardTitle>运行参数</CardTitle>
                        <Help
                          label="运行参数说明"
                          text="新建表单预填可见的常用起始值；保存时这些值会作为任务配置提交，服务端不会对缺失字段静默补值。"
                        />
                      </CardHeader>
                      <CardContent className="grid gap-4 sm:grid-cols-2">
                        {(
                          [
                            ['batchSize', '批次大小'],
                            ['bufferSize', '缓冲区大小'],
                            ['checkpointIntervalSeconds', '检查点间隔（秒）'],
                          ] as const
                        ).map(([key, label]) => (
                          <Field key={key} label={label}>
                            <Input
                              type="number"
                              aria-label={label}
                              min={1}
                              step={1}
                              value={draft[key] ?? ''}
                              onChange={(event) =>
                                update(
                                  key,
                                  event.target.value === '' ? null : Number(event.target.value),
                                )
                              }
                            />
                          </Field>
                        ))}
                        {draft.taskType === 'KAFKA_KAFKA' && (
                          <Field label="ACK 间隔（秒）">
                            <Input
                              type="number"
                              aria-label="ACK 间隔（秒）"
                              min={1}
                              step={1}
                              value={draft.ackIntervalSeconds ?? ''}
                              onChange={(event) =>
                                update(
                                  'ackIntervalSeconds',
                                  event.target.value === '' ? null : Number(event.target.value),
                                )
                              }
                            />
                          </Field>
                        )}
                        <Field label="Kafka required acks">
                          <select
                            className={selectStyle}
                            aria-label="Kafka required acks"
                            value={draft.requiredAcks}
                            onChange={(event) =>
                              update('requiredAcks', event.target.value as RequiredAcks)
                            }
                          >
                            <option value="all">all（推荐）</option>
                            <option value="one">one</option>
                          </select>
                        </Field>
                      </CardContent>
                    </Card>
                  </div>
                )}
                {step === 3 && (
                  <Card className="task-editor-confirm">
                    <CardContent className="task-editor-confirm-grid">
                      <div className="space-y-4">
                        <dl className="grid gap-4 text-sm sm:grid-cols-2">
                          <div>
                            <dt className="text-muted-foreground">任务名称</dt>
                            <dd>{draft.name || '未填写'}</dd>
                          </div>
                          <div>
                            <dt className="text-muted-foreground">任务类型</dt>
                            <dd>{taskTypeLabel(draft.taskType)}</dd>
                          </div>
                          <div>
                            <dt className="text-muted-foreground">运行制品</dt>
                            <dd>
                              {artifacts.find((item) => item.id === draft.artifactId)
                                ?.versionIdentifier ??
                                draft.artifactId ??
                                '未绑定'}
                            </dd>
                          </div>
                          <div>
                            <dt className="text-muted-foreground">Topic 策略</dt>
                            <dd className="break-all">
                              {mappingTargets(draft.mappings).some((item) => item.autoCreateTopic)
                                ? `首次启动时创建缺失 Topic（${mappingTargets(draft.mappings)
                                    .filter((item) => item.autoCreateTopic)
                                    .map(
                                      (item) =>
                                        `${item.topic}：分区 ${item.numPartitions ?? '未填写'}，副本 ${item.replicationFactor ?? '未填写'}`,
                                    )
                                    .join('；')}）`
                                : '使用已有 Topic'}
                            </dd>
                          </div>
                          {draft.taskType !== 'KAFKA_KAFKA' && (
                            <>
                              <div>
                                <dt className="text-muted-foreground">初始快照</dt>
                                <dd>{draft.includeSnapshot ? '包含' : '不包含'}</dd>
                              </div>
                              <div>
                                <dt className="text-muted-foreground">消息编码</dt>
                                <dd>
                                  {draft.messageFormat === 'json'
                                    ? `JSON / ${draft.jsonTemplate === 'cloudcanal' ? 'CloudCanal' : draft.jsonTemplate === 'standard' ? '标准 JSON' : '制品默认模板'}`
                                    : draft.messageFormat === 'avro'
                                      ? 'Avro'
                                      : '制品默认（Avro）'}
                                </dd>
                              </div>
                              {draft.taskType === 'PG_KAFKA' && (
                                <div>
                                  <dt className="text-muted-foreground">复制资源</dt>
                                  <dd>
                                    {draft.resourceMode === 'SELF_MANAGED'
                                      ? '使用已有资源'
                                      : draft.resourceMode === 'PLATFORM_MANAGED'
                                        ? '平台管理（显式准备）'
                                        : '引擎管理'}
                                  </dd>
                                </div>
                              )}
                            </>
                          )}
                        </dl>
                        <p className="text-sm text-muted-foreground">
                          保存草稿后运行预检。预检不启动任务，也不创建 Topic。
                        </p>
                      </div>
                      <MessageEncodingPreview
                        title="预计消息内容"
                        taskType={draft.taskType}
                        database={sourceDatabase}
                        mappings={draft.mappings}
                        messageFormat={draft.messageFormat}
                        jsonTemplate={draft.jsonTemplate}
                        withFieldDefs={draft.withFieldDefs}
                      />
                    </CardContent>
                  </Card>
                )}
                {preflight !== null && (
                  <PreflightResult
                    run={preflight}
                    pgAction={
                      <Button
                        className="mt-3"
                        variant="outline"
                        disabled={preparingPg || saving || checking}
                        onClick={() => {
                          if (!canPreparePg) {
                            setStep(0);
                            return;
                          }
                          setError(null);
                          setNotice(null);
                          setPgRiskConfirmed(false);
                          setPreparePgOpen(true);
                        }}
                      >
                        {canPreparePg
                          ? draft.resourceMode === 'SELF_MANAGED'
                            ? '核验并绑定复制资源'
                            : '准备复制资源'
                          : '查看来源配置'}
                      </Button>
                    }
                  />
                )}
                {session.user.role === 'ADMIN' &&
                  mappingTargets(draft.mappings).some((item) => item.autoCreateTopic) &&
                  topicPending && (
                    <div className="space-y-2">
                      <Button
                        disabled={topicPreparationBlocked !== null}
                        onClick={() => setPrepareTopicOpen(true)}
                      >
                        准备 Topic 并重新预检
                      </Button>
                      {topicPreparationBlocked !== null && (
                        <p className="text-sm text-muted-foreground">{topicPreparationBlocked}</p>
                      )}
                    </div>
                  )}
              </div>
              <footer className="task-editor-footer" aria-label="任务编辑操作">
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    disabled={step === 0}
                    onClick={() => setStep((value) => Math.max(0, value - 1))}
                  >
                    上一步
                  </Button>
                  {step < 3 && (
                    <Button
                      onClick={() => {
                        try {
                          if (step === 1)
                            serializeTaskMappings(draft.mappings, draft.taskType, true);
                          setStep((value) => Math.min(3, value + 1));
                        } catch (error) {
                          reportFailure('映射配置不完整', error);
                        }
                      }}
                    >
                      下一步
                    </Button>
                  )}
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <Button
                    type="button"
                    disabled={
                      (!dirty && !(selectedTask?.draft === null && selectedTask.applied != null)) ||
                      checking ||
                      saving ||
                      starting ||
                      groupBusy
                    }
                    onClick={() => void save()}
                  >
                    {saving ? <Loader2 className="animate-spin" /> : <Save />}
                    {selectedTask === null ? '创建并保存草稿' : '保存草稿'}
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    disabled={
                      selectedTask === null ||
                      (selectedTask.draft === null &&
                        !(
                          selectedTask.applied != null &&
                          selectedTask.desiredState === 'STOPPED' &&
                          selectedTask.observedState === 'STOPPED'
                        )) ||
                      selectedTask.draft?.state === 'INCOMPLETE_DRAFT' ||
                      dirty ||
                      saving ||
                      checking
                    }
                    onClick={() => void runPreflight()}
                  >
                    {checking ? <Loader2 className="animate-spin" /> : <ShieldCheck />}运行预检
                  </Button>
                  {selectedTask !== null &&
                    ['PREFLIGHT_PASSED', 'RISK_CONFIRMED'].includes(
                      selectedTask.draft?.state ?? '',
                    ) &&
                    selectedTask.observedState === 'STOPPED' && (
                      <Button
                        type="button"
                        disabled={
                          dirty || saving || checking || starting || reinitializationPreflight
                        }
                        onClick={() => void startTask()}
                      >
                        {starting && <Loader2 className="animate-spin" />}
                        启动任务
                      </Button>
                    )}
                  {selectedTask?.taskType === 'MYSQL_KAFKA' &&
                    selectedTask.desiredState === 'STOPPED' &&
                    selectedTask.observedState === 'STOPPED' && (
                      <Button
                        type="button"
                        variant="outline"
                        disabled={dirty || saving || checking || starting}
                        onClick={() => openMysqlReinitialization(selectedTask)}
                      >
                        重新全量
                      </Button>
                    )}
                  {reinitializationPreflight && selectedTask?.observedState === 'STOPPED' && (
                    <span className="text-xs text-muted-foreground">
                      普通续传需重新运行常规预检。
                    </span>
                  )}
                  {selectedTask?.desiredState === 'STOPPED' &&
                    deletableObservedStates.has(selectedTask.observedState) && (
                      <Button
                        type="button"
                        variant="destructive"
                        disabled={dirty || saving || checking}
                        onClick={() => openDeletePrompt(selectedTask)}
                      >
                        <Trash2 />
                        删除任务
                      </Button>
                    )}
                  {dirty && (
                    <span className="text-sm text-amber-700 dark:text-amber-300">
                      当前有未保存修改
                    </span>
                  )}
                </div>
              </footer>
            </section>
          )}
          <TaskPrompt
            title={groupPrompt === 'RENAME' ? '重命名任务组' : '新增任务组'}
            open={groupPrompt !== null}
            busy={groupBusy}
            onClose={() => setGroupPrompt(null)}
          >
            <form
              onSubmit={(event) => {
                event.preventDefault();
                void createGroup();
              }}
              className="space-y-4"
            >
              <Field label="任务组名称">
                <Input
                  aria-label="任务组名称"
                  value={groupName}
                  maxLength={128}
                  autoFocus
                  required
                  disabled={groupBusy}
                  onChange={(event) => setGroupName(event.target.value)}
                />
              </Field>
              <DialogFooter>
                <Button
                  type="button"
                  variant="outline"
                  disabled={groupBusy}
                  onClick={() => setGroupPrompt(null)}
                >
                  取消
                </Button>
                <Button type="submit" disabled={groupBusy || !groupName.trim()}>
                  {groupBusy && <Loader2 className="animate-spin" />}
                  {groupPrompt === 'RENAME'
                    ? '保存名称'
                    : groupPrompt === 'EDITOR'
                      ? '创建并选择'
                      : '创建任务组'}
                </Button>
              </DialogFooter>
            </form>
          </TaskPrompt>
          <TaskPrompt
            title="删除任务组"
            open={deletingGroup !== null}
            busy={groupBusy}
            onClose={() => setDeletingGroup(null)}
          >
            <p className="text-sm">
              确定删除任务组「{deletingGroup?.group.name}」？此操作不删除同步任务及其历史记录。
            </p>
            {deletingGroup?.occupied && (
              <p className="text-sm text-amber-700 dark:text-amber-300">
                该分组内仍有任务。继续删除将解除这些任务的分组关系，任务保留在“全部任务”中，运行不受影响。
              </p>
            )}
            <DialogFooter>
              <Button variant="outline" disabled={groupBusy} onClick={() => setDeletingGroup(null)}>
                取消
              </Button>
              <Button variant="destructive" disabled={groupBusy} onClick={() => void deleteGroup()}>
                {groupBusy && <Loader2 className="animate-spin" />}
                {deletingGroup?.occupied ? '删除分组并保留任务' : '删除分组'}
              </Button>
            </DialogFooter>
          </TaskPrompt>
          <TaskPrompt
            title="确认新增同步表"
            open={pgAddition !== null}
            busy={saving}
            onClose={() => setPgAddition(null)}
          >
            <p className="text-sm">以下新增表仅同步增量，不扫描存量数据：</p>
            <ul className="max-h-48 list-inside list-disc overflow-y-auto break-all text-sm">
              {pgAddition?.addedTables.map((table) => (
                <li key={JSON.stringify([table.schema, table.name])}>
                  {table.schema}.{table.name}
                </li>
              ))}
            </ul>
            <p className="text-sm text-muted-foreground">
              将保留原平台任务、复制槽和历史记录，全部表从原复制槽位点续传；如存在 WAL
              积压，新增表可能收到添加前尚未消费的变更。本次保存不会启动任务，后续需准备复制资源、运行预检后再启动。
            </p>
            <DialogFooter>
              <Button variant="outline" disabled={saving} onClick={() => setPgAddition(null)}>
                取消
              </Button>
              <Button disabled={saving} onClick={confirmPgTableAddition}>
                {saving && <Loader2 className="animate-spin" />}
                {saving ? '正在保存…' : '确认并保存草稿'}
              </Button>
            </DialogFooter>
          </TaskPrompt>
          <TaskPrompt
            title="确认 PostgreSQL 复制资源"
            open={preparePgOpen}
            busy={preparingPg}
            onClose={() => {
              setPreparePgOpen(false);
              setPgRiskConfirmed(false);
            }}
          >
            <dl className="space-y-2 break-all text-sm">
              <div>
                源库：{sourceDataSource?.name} / {sourceDatabase}
              </div>
              <div>
                源表：
                {draft.mappings.map((row) => mappingSourceLabel(row, draft.taskType)).join('、')}
              </div>
              <div>Slot：{draft.slotName}</div>
              <div>Publication：{draft.publicationName}</div>
            </dl>
            <p className="text-sm">
              {draft.resourceMode === 'SELF_MANAGED'
                ? '仅核验并绑定已有资源，不创建或修改。'
                : pgExpansion
                  ? '仅为原 Publication 添加缺失的新增表；保留原复制槽及位点，不创建或重建复制资源。'
                  : '创建缺失资源，并核验、绑定已有资源；不修改已有复制槽或发布。'}
              准备后重新预检，不启动同步。
            </p>
            <p className="text-sm text-muted-foreground">
              {pgExpansion
                ? '原复制槽从已确认位点续传，不补存量；WAL 积压时新增表可能收到添加前尚未消费的变更。Publication 扩展成功后不会因后续失败自动撤销，可核实后重试。'
                : '新建复制槽从创建时的当前位置开始采集，不能补回此前的变更；未消费的复制槽会保留 WAL 并占用源库磁盘。外部资源创建成功后不会因后续失败而自动删除。'}
            </p>
            <label className="flex items-start gap-2 text-sm">
              <input
                type="checkbox"
                checked={pgRiskConfirmed}
                onChange={(event) => setPgRiskConfirmed(event.target.checked)}
                disabled={preparingPg}
              />
              我已核对资源名称，并理解起始位置与 WAL 保留风险。
            </label>
            <DialogFooter>
              <Button
                variant="outline"
                disabled={preparingPg}
                onClick={() => setPreparePgOpen(false)}
              >
                取消
              </Button>
              <Button
                disabled={!pgRiskConfirmed || preparingPg}
                onClick={() => void preparePgResources()}
              >
                {preparingPg && <Loader2 className="animate-spin" />}
                {preparingPg ? '正在准备并预检…' : '确认复制资源'}
              </Button>
            </DialogFooter>
          </TaskPrompt>
          <TaskPrompt
            title="确认准备目标 Topic"
            open={prepareTopicOpen}
            busy={starting}
            onClose={() => setPrepareTopicOpen(false)}
          >
            <p className="break-all text-sm">
              将为当前已保存修订准备目标 Topic「
              {mappingTargets(draft.mappings)
                .filter((item) => item.autoCreateTopic)
                .map((item) => item.topic)
                .join('、')}
              」，并重新预检。准备成功并通过预检后可直接启动同步。
            </p>
            <DialogFooter>
              <Button
                variant="outline"
                disabled={starting}
                onClick={() => setPrepareTopicOpen(false)}
              >
                取消
              </Button>
              <Button disabled={starting} onClick={() => void prepareTargetTopic()}>
                {starting && <Loader2 className="animate-spin" />}确认准备
              </Button>
            </DialogFooter>
          </TaskPrompt>
          <TaskPrompt
            title="确认创建 Topic"
            open={topicPrompt}
            busy={creatingTopic}
            onClose={() => setTopicPrompt(false)}
          >
            <p className="break-all text-sm">
              将在「{targetDataSource?.name}」创建「{selectedTopic?.topic}」，分区数{' '}
              {selectedTopic?.numPartitions}
              ，副本数 {selectedTopic?.replicationFactor}。
            </p>
            <p className="text-sm text-muted-foreground">
              已有 Topic 不修改。创建成功后，即使放弃任务草稿，Topic 仍会保留。
            </p>
            <DialogFooter>
              <Button
                variant="outline"
                disabled={creatingTopic}
                onClick={() => setTopicPrompt(false)}
              >
                取消
              </Button>
              <Button disabled={creatingTopic} onClick={() => void createTopic()}>
                {creatingTopic && <Loader2 className="animate-spin" />}确认创建
              </Button>
            </DialogFooter>
          </TaskPrompt>
          <Dialog
            open={detailTask !== null}
            onOpenChange={(open) => {
              if (!open) setDetailTask(null);
            }}
          >
            <DialogContent
              className="task-detail-dialog flex w-[calc(100vw-2rem)] flex-col overflow-hidden sm:max-w-[1280px]"
              style={{ height: '90vh', maxHeight: '90vh' }}
              onOpenAutoFocus={(event) => {
                event.preventDefault();
                detailHeading.current?.focus();
              }}
            >
              <DialogHeader className="shrink-0">
                <DialogTitle ref={detailHeading} tabIndex={-1}>
                  任务详情
                </DialogTitle>
                <DialogDescription className="flex flex-wrap items-center gap-3 text-lg font-semibold text-foreground">
                  {detailTask?.name}
                  {detailTask && (
                    <Badge
                      variant="outline"
                      className={observedStateBadgeClassNames[detailTask.observedState]}
                    >
                      {operationName(detailTask.observedState)}
                    </Badge>
                  )}
                </DialogDescription>
              </DialogHeader>
              {detailTask && (
                <div
                  className="flex shrink-0 flex-wrap items-center gap-4 rounded-lg bg-muted/40 px-4 py-3 text-sm"
                  aria-label="同步链路"
                >
                  <Database className="size-5 text-muted-foreground" aria-hidden="true" />
                  <span>
                    <span className="text-muted-foreground">Source · </span>
                    {dataSources.find((source) => source.id === detailTask.sourceDataSourceId)
                      ?.name ?? '数据源不可用'}
                  </span>
                  <ArrowRight className="size-5 text-muted-foreground" aria-label="同步至" />
                  <span>
                    <span className="text-muted-foreground">Sink · </span>
                    {dataSources.find((source) => source.id === detailTask.targetDataSourceId)
                      ?.name ?? '数据源不可用'}
                  </span>
                  <span className="text-xs text-muted-foreground sm:ml-auto">
                    {taskTypeLabel(detailTask.taskType)}
                  </span>
                </div>
              )}
              {detailTask && (
                <TaskRuntimeDetails
                  key={detailTask.id}
                  task={detailTask}
                  dataSources={dataSources}
                  artifacts={artifacts}
                  onSessionInvalidated={onSessionInvalidated}
                  onTaskChanged={async () => {
                    const result = await tasksApi.list();
                    setTasks(result.tasks);
                    setDetailTask((current) =>
                      current === null || current.id !== detailTask.id
                        ? current
                        : (result.tasks.find((item) => item.id === current.id) ?? null),
                    );
                  }}
                />
              )}
              <DialogFooter className="shrink-0">
                <Button type="button" variant="outline" onClick={() => setDetailTask(null)}>
                  关闭
                </Button>
                {detailTask?.taskType === 'MYSQL_KAFKA' &&
                  detailTask.desiredState === 'STOPPED' &&
                  detailTask.observedState === 'STOPPED' && (
                    <Button
                      type="button"
                      variant="outline"
                      disabled={starting}
                      onClick={() => openMysqlReinitialization(detailTask)}
                    >
                      重新全量
                    </Button>
                  )}
                <Button
                  type="button"
                  onClick={() => {
                    if (detailTask) selectTask(detailTask);
                    setDetailTask(null);
                  }}
                >
                  编辑任务
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
          <TaskPrompt
            title="重新执行 MySQL 全量同步？"
            open={reinitializeTarget !== null}
            busy={starting}
            onClose={() => setReinitializeTarget(null)}
          >
            <p className="text-sm">
              将重新读取 {reinitializeTarget?.name} 的全部来源表，并从新的 Binlog
              起点继续增量同步，可能产生重复数据。
            </p>
            <p className="text-sm text-muted-foreground">
              原检查点仅在预检通过并受理启动时重置。不会删除源库数据、Kafka 消息或历史运行。
            </p>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                disabled={starting}
                onClick={() => setReinitializeTarget(null)}
              >
                取消
              </Button>
              <Button
                type="button"
                disabled={starting}
                onClick={() => void reinitializeMysqlSnapshot()}
              >
                {starting && <Loader2 className="animate-spin" />}确认重新全量
              </Button>
            </DialogFooter>
          </TaskPrompt>
          <TaskPrompt
            title={listRuntimePrompt?.action === 'RESTART' ? '确认重新启动任务' : '确认停止任务'}
            open={listRuntimePrompt !== null}
            busy={listRuntimeBusy}
            onClose={() => setListRuntimePrompt(null)}
          >
            <p className="text-sm">
              {listRuntimePrompt?.action === 'RESTART'
                ? `将受控停止“${listRuntimePrompt.task.name}”，预检通过后按已应用配置续传，不重新全量。同步会短暂中断。`
                : `将受控停止“${listRuntimePrompt?.task.name ?? ''}”，保留同步位点和历史记录。`}
            </p>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                disabled={listRuntimeBusy}
                onClick={() => setListRuntimePrompt(null)}
              >
                取消
              </Button>
              <Button
                type="button"
                disabled={listRuntimeBusy}
                onClick={() => {
                  if (listRuntimePrompt !== null)
                    void runListRuntimeAction(listRuntimePrompt.task, listRuntimePrompt.action);
                }}
              >
                {listRuntimeBusy && <Loader2 className="animate-spin" />}
                {listRuntimePrompt?.action === 'RESTART' ? '确认重新启动' : '确认停止'}
              </Button>
            </DialogFooter>
          </TaskPrompt>
          <TaskPrompt
            title="确认删除任务"
            open={deleteOpen}
            busy={deleting}
            onClose={clearDeletePrompt}
          >
            <p className="text-sm">
              将删除任务“{deleteTarget?.name ?? '-'}”。任务将不再显示在任务列表中。
            </p>
            <p className="text-sm text-muted-foreground">
              历史修订、操作记录和审计记录仍由系统保留。
            </p>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                disabled={deleting}
                onClick={clearDeletePrompt}
              >
                取消
              </Button>
              <Button
                type="button"
                variant="destructive"
                disabled={deleting}
                onClick={() => void deleteTask()}
              >
                {deleting && <Loader2 className="animate-spin" />}确认删除
              </Button>
            </DialogFooter>
          </TaskPrompt>
          <TaskPrompt
            title={recoveryAction === 'RETRY' ? '恢复同步任务' : '放弃旧运行恢复'}
            open={discardTarget !== null}
            busy={discarding}
            onClose={() => setDiscardTarget(null)}
          >
            {recoveryAction === 'RETRY' ? (
              <p className="text-sm">
                将重新检查任务“{discardTarget?.name}
                ”的原配置、制品及同步连续性，检查通过后恢复运行。
              </p>
            ) : (
              <p className="text-sm">
                将任务“{discardTarget?.name}
                ”置为停止并释放旧运行占用。任务配置、失败记录及同步位点保留，之后需重新预检。
              </p>
            )}
            <p className="text-sm text-muted-foreground">
              本操作不终止其他容器中的进程，也不删除复制槽、Publication 或 Kafka Topic。
            </p>
            <label className="flex items-start gap-2 text-sm">
              <input
                type="checkbox"
                checked={discardConfirmed}
                disabled={discarding}
                onChange={(event) => setDiscardConfirmed(event.target.checked)}
              />
              我已确认旧容器及该任务的旧进程已停止
            </label>
            <DialogFooter>
              <Button
                variant="outline"
                disabled={discarding}
                onClick={() => setDiscardTarget(null)}
              >
                取消
              </Button>
              <Button
                disabled={!discardConfirmed || discarding}
                onClick={() => void discardRecovery()}
              >
                {discarding && <Loader2 className="animate-spin" />}
                {recoveryAction === 'RETRY' ? '确认恢复任务' : '确认放弃恢复'}
              </Button>
            </DialogFooter>
          </TaskPrompt>
          {prompt}
        </div>
      )}
    </>
  );
}
