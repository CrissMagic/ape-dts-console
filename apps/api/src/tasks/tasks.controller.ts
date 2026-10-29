import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Delete,
  ForbiddenException,
  Get,
  Param,
  Patch,
  Post,
  Req,
  NotFoundException,
  UseGuards,
} from '@nestjs/common';
import {
  assignTaskGroupRequestSchema,
  createTaskGroupRequestSchema,
  taskGroupIdentifierSchema,
  renameTaskGroupRequestSchema,
  deleteTaskGroupRequestSchema,
  reorderTaskGroupsRequestSchema,
  createTaskRequestSchema,
  deleteTaskRequestSchema,
  saveTaskDraftRequestSchema,
  taskIdentifierParametersSchema,
} from '@scs/contracts';
import { ZodError, type ZodType } from 'zod';

import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { TasksService, TasksServiceError } from './tasks.service.js';

function parse<T>(schema: ZodType<T>, value: unknown): T {
  try {
    return schema.parse(value);
  } catch (error) {
    if (error instanceof ZodError) {
      throw new BadRequestException({
        code: 'INVALID_REQUEST',
        issues: error.issues.map(({ code, path }) => ({ code, path })),
      });
    }
    throw error;
  }
}

function mapTasksError(error: unknown): never {
  if (
    error instanceof Error &&
    'code' in error &&
    error.code === '23514' &&
    error.message === 'artifact unavailable'
  ) {
    throw new ConflictException({ code: 'TASK_ARTIFACT_NOT_AVAILABLE' });
  }
  if (error instanceof TasksServiceError) {
    switch (error.code) {
      case 'TASK_ACCESS_DENIED':
        throw new ForbiddenException({ code: error.code });
      case 'TASK_NOT_FOUND':
      case 'TASK_GROUP_NOT_FOUND':
      case 'TASK_DATA_SOURCE_NOT_FOUND':
        throw new NotFoundException({ code: error.code });
      case 'TASK_ACTIVE_RUN_UNITS':
      case 'TASK_ARTIFACT_NOT_AVAILABLE':
      case 'TASK_CONFIGURATION_DATA_SOURCE_MISMATCH':
      case 'TASK_CONFIGURATION_INVALID':
      case 'TASK_DRAFT_VERSION_CONFLICT':
      case 'TASK_GROUP_NAME_ALREADY_EXISTS':
      case 'TASK_GROUP_ASSIGNMENT_CONFLICT':
      case 'TASK_GROUP_CHANGED':
      case 'TASK_GROUP_NOT_EMPTY':
      case 'TASK_NAME_ALREADY_EXISTS':
      case 'TASK_NOT_STOPPED':
      case 'TASK_PG_TABLE_ADDITION_CONFIRMATION_REQUIRED':
      case 'TASK_PG_TABLE_ADDITION_NOT_ALLOWED':
      case 'TASK_REVISION_IMMUTABLE':
      case 'TASK_DATA_SOURCE_TYPE_INVALID':
      case 'TASK_TYPE_CONFIGURATION_MISMATCH':
      case 'TASK_TYPE_IMMUTABLE':
      case 'TASK_VERSION_CONFLICT':
        throw new ConflictException({ code: error.code });
    }
  }
  throw error;
}

@Controller('tasks')
export class TasksController {
  constructor(private readonly tasks: TasksService) {}

  @Get()
  @UseGuards(SessionAuthGuard)
  async list(@Req() request: AuthenticatedFastifyRequest) {
    try {
      return { tasks: await this.tasks.list(requireAuthContext(request).user) };
    } catch (error) {
      mapTasksError(error);
    }
  }

  @Get(':taskId')
  @UseGuards(SessionAuthGuard)
  async get(@Param() parameters: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const { taskId } = parse(taskIdentifierParametersSchema, parameters);
    try {
      return await this.tasks.get(requireAuthContext(request).user, taskId);
    } catch (error) {
      mapTasksError(error);
    }
  }

  @Post()
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async create(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    try {
      return await this.tasks.create(
        requireAuthContext(request).user,
        parse(createTaskRequestSchema, body),
      );
    } catch (error) {
      mapTasksError(error);
    }
  }

  @Patch(':taskId/draft')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async saveDraft(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(taskIdentifierParametersSchema, parameters);
    try {
      return await this.tasks.saveDraft(
        requireAuthContext(request).user,
        taskId,
        parse(saveTaskDraftRequestSchema, body),
      );
    } catch (error) {
      mapTasksError(error);
    }
  }

  @Patch(':taskId/group')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async assignGroup(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(taskIdentifierParametersSchema, parameters);
    try {
      return await this.tasks.assignGroup(
        requireAuthContext(request).user,
        taskId,
        parse(assignTaskGroupRequestSchema, body),
      );
    } catch (error) {
      mapTasksError(error);
    }
  }

  @Delete(':taskId')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async delete(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { taskId } = parse(taskIdentifierParametersSchema, parameters);
    try {
      return await this.tasks.delete(
        requireAuthContext(request).user,
        taskId,
        parse(deleteTaskRequestSchema, body).expectedRowVersion,
      );
    } catch (error) {
      mapTasksError(error);
    }
  }
}

@Controller('task-groups')
export class TaskGroupsController {
  constructor(private readonly tasks: TasksService) {}

  @Get()
  @UseGuards(SessionAuthGuard)
  async list() {
    return { groups: await this.tasks.listGroups() };
  }

  @Post()
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async create(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    try {
      return await this.tasks.createGroup(
        requireAuthContext(request).user,
        parse(createTaskGroupRequestSchema, body),
      );
    } catch (error) {
      mapTasksError(error);
    }
  }

  @Patch('order')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async reorder(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    try {
      return await this.tasks.reorderGroups(
        requireAuthContext(request).user,
        parse(reorderTaskGroupsRequestSchema, body),
      );
    } catch (error) {
      mapTasksError(error);
    }
  }

  @Patch(':groupId')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async rename(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { groupId } = parse(taskGroupIdentifierSchema, parameters);
    try {
      return await this.tasks.renameGroup(
        requireAuthContext(request).user,
        groupId,
        parse(renameTaskGroupRequestSchema, body),
      );
    } catch (error) {
      mapTasksError(error);
    }
  }

  @Delete(':groupId')
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async delete(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { groupId } = parse(taskGroupIdentifierSchema, parameters);
    try {
      return await this.tasks.deleteGroup(
        requireAuthContext(request).user,
        groupId,
        parse(deleteTaskGroupRequestSchema, body),
      );
    } catch (error) {
      mapTasksError(error);
    }
  }
}
