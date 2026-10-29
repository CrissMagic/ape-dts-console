import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Delete,
  ForbiddenException,
  Get,
  HttpCode,
  HttpException,
  HttpStatus,
  NotFoundException,
  Param,
  Patch,
  Post,
  Query,
  Req,
  ServiceUnavailableException,
  UseGuards,
} from '@nestjs/common';
import {
  createDataSourceGrantRequestSchema,
  createKafkaTopicRequestSchema,
  createDataSourceRequestSchema,
  copyDataSourceRequestSchema,
  dataSourceCatalogQuerySchema,
  dataSourceGrantParametersSchema,
  dataSourceIdentifierParametersSchema,
  deleteDataSourceRequestSchema,
  metadataQuerySchema,
  mysqlCdcCheckRequestSchema,
  updateDataSourceRequestSchema,
} from '@scs/contracts';
import { ZodError, type ZodType } from 'zod';

import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import {
  DataSourcesService,
  DataSourcesServiceError,
  KafkaTopicAuditError,
} from './data-sources.service.js';
import { DataSourceAdapterError } from './adapters/types.js';

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

function mapDataSourcesError(error: unknown): never {
  if (error instanceof KafkaTopicAuditError) {
    throw new ServiceUnavailableException({
      code: error.code,
      message: error.message,
      result: error.result,
    });
  }
  if (error instanceof DataSourceAdapterError) {
    const payload = { code: error.code, message: error.summary };
    if (error.code === 'PERMISSION_DENIED') throw new ForbiddenException(payload);
    if (error.code === 'KAFKA_TOPIC_PARAMETERS_INVALID' || error.code === 'METADATA_UNSUPPORTED')
      throw new BadRequestException(payload);
    throw new ServiceUnavailableException(payload);
  }
  if (error instanceof HttpException) {
    throw error;
  }
  if (error instanceof DataSourcesServiceError) {
    switch (error.code) {
      case 'DATA_SOURCE_ACCESS_DENIED':
        throw new ForbiddenException({ code: error.code });
      case 'DATA_SOURCE_NOT_FOUND':
      case 'DATA_SOURCE_GRANT_NOT_FOUND':
        throw new NotFoundException({ code: error.code });
      case 'DATA_SOURCE_CHANGED_DURING_OPERATION':
      case 'DATA_SOURCE_GRANT_ALREADY_EXISTS':
      case 'DATA_SOURCE_GRANT_TARGET_INVALID':
      case 'DATA_SOURCE_NAME_ALREADY_EXISTS':
      case 'DATA_SOURCE_REFERENCED_BY_RESOURCE':
      case 'DATA_SOURCE_REFERENCED_BY_TASK':
      case 'DATA_SOURCE_TYPE_IMMUTABLE':
      case 'DATA_SOURCE_VERSION_CONFLICT':
        throw new ConflictException({ code: error.code });
      case 'DATA_SOURCE_CONFIGURATION_UNAVAILABLE':
      case 'DATA_SOURCE_SECRET_UNAVAILABLE':
      case 'SECURITY_POLICY_UNAVAILABLE':
        throw new ServiceUnavailableException({ code: error.code });
    }
  }
  throw error;
}

const ADMIN_MUTATION_GUARDS = [SessionAuthGuard, CsrfGuard, AdminGuard] as const;

@Controller('data-sources')
export class DataSourcesController {
  constructor(private readonly dataSources: DataSourcesService) {}

  @Post(':dataSourceId/topics')
  @HttpCode(HttpStatus.OK)
  @UseGuards(...ADMIN_MUTATION_GUARDS)
  async createTopic(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { dataSourceId } = parse(dataSourceIdentifierParametersSchema, parameters);
    try {
      return await this.dataSources.createKafkaTopic(
        requireAuthContext(request).user,
        dataSourceId,
        parse(createKafkaTopicRequestSchema, body),
      );
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Get()
  @UseGuards(SessionAuthGuard)
  async list(@Req() request: AuthenticatedFastifyRequest) {
    try {
      return { dataSources: await this.dataSources.list(requireAuthContext(request).user) };
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Get('catalog')
  @UseGuards(SessionAuthGuard)
  async catalog(@Query() query: unknown, @Req() request: AuthenticatedFastifyRequest) {
    try {
      return await this.dataSources.catalog(
        requireAuthContext(request).user,
        parse(dataSourceCatalogQuerySchema, query),
      );
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Post()
  @UseGuards(...ADMIN_MUTATION_GUARDS)
  async create(@Body() body: unknown, @Req() request: AuthenticatedFastifyRequest) {
    try {
      return await this.dataSources.create(
        requireAuthContext(request).user.id,
        parse(createDataSourceRequestSchema, body),
      );
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Post(':dataSourceId/copy')
  @UseGuards(...ADMIN_MUTATION_GUARDS)
  async copy(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { dataSourceId } = parse(dataSourceIdentifierParametersSchema, parameters);
    try {
      return await this.dataSources.copy(
        requireAuthContext(request).user.id,
        dataSourceId,
        parse(copyDataSourceRequestSchema, body),
      );
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Patch(':dataSourceId')
  @UseGuards(...ADMIN_MUTATION_GUARDS)
  async update(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { dataSourceId } = parse(dataSourceIdentifierParametersSchema, parameters);
    try {
      return await this.dataSources.update(
        requireAuthContext(request).user.id,
        dataSourceId,
        parse(updateDataSourceRequestSchema, body),
      );
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Delete(':dataSourceId')
  @UseGuards(...ADMIN_MUTATION_GUARDS)
  async delete(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { dataSourceId } = parse(dataSourceIdentifierParametersSchema, parameters);
    const { expectedRowVersion } = parse(deleteDataSourceRequestSchema, body);
    try {
      return await this.dataSources.delete(
        requireAuthContext(request).user.id,
        dataSourceId,
        expectedRowVersion,
      );
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Get(':dataSourceId/grants')
  @UseGuards(SessionAuthGuard, AdminGuard)
  async listGrants(@Param() parameters: unknown) {
    const { dataSourceId } = parse(dataSourceIdentifierParametersSchema, parameters);
    try {
      return { grants: await this.dataSources.listGrants(dataSourceId) };
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Post(':dataSourceId/grants')
  @UseGuards(...ADMIN_MUTATION_GUARDS)
  async grant(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { dataSourceId } = parse(dataSourceIdentifierParametersSchema, parameters);
    try {
      return await this.dataSources.grant(
        requireAuthContext(request).user,
        dataSourceId,
        parse(createDataSourceGrantRequestSchema, body),
      );
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Delete(':dataSourceId/grants/:userId')
  @HttpCode(HttpStatus.NO_CONTENT)
  @UseGuards(...ADMIN_MUTATION_GUARDS)
  async revoke(
    @Param() parameters: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ): Promise<void> {
    const { dataSourceId, userId } = parse(dataSourceGrantParametersSchema, parameters);
    try {
      await this.dataSources.revoke(requireAuthContext(request).user.id, dataSourceId, userId);
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Post(':dataSourceId/test')
  @HttpCode(HttpStatus.OK)
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async test(@Param() parameters: unknown, @Req() request: AuthenticatedFastifyRequest) {
    const { dataSourceId } = parse(dataSourceIdentifierParametersSchema, parameters);
    try {
      return await this.dataSources.test(requireAuthContext(request).user, dataSourceId);
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Post(':dataSourceId/metadata')
  @HttpCode(HttpStatus.OK)
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async metadata(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { dataSourceId } = parse(dataSourceIdentifierParametersSchema, parameters);
    try {
      return await this.dataSources.metadata(
        requireAuthContext(request).user,
        dataSourceId,
        parse(metadataQuerySchema, body),
      );
    } catch (error) {
      mapDataSourcesError(error);
    }
  }

  @Post(':dataSourceId/mysql-cdc-check')
  @HttpCode(HttpStatus.OK)
  @UseGuards(SessionAuthGuard, CsrfGuard)
  async inspectMysqlCdc(
    @Param() parameters: unknown,
    @Body() body: unknown,
    @Req() request: AuthenticatedFastifyRequest,
  ) {
    const { dataSourceId } = parse(dataSourceIdentifierParametersSchema, parameters);
    try {
      return await this.dataSources.inspectMysqlCdc(
        requireAuthContext(request).user,
        dataSourceId,
        parse(mysqlCdcCheckRequestSchema, body).expectedRowVersion,
      );
    } catch (error) {
      mapDataSourcesError(error);
    }
  }
}
