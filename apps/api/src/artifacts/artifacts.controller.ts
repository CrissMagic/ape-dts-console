import {
  BadRequestException,
  Body,
  ConflictException,
  Controller,
  Delete,
  Get,
  Param,
  Post,
  Req,
  Res,
  UseGuards,
} from '@nestjs/common';
import { Readable } from 'node:stream';
import type { FastifyReply } from 'fastify';
import {
  registerArtifactRequestSchema,
  artifactFileCandidateSchema,
  registerArtifactFileRequestSchema,
} from '@scs/contracts';
import { ArtifactIntegrityError, ElfInspectionError, LocalStoragePathError } from '@scs/storage';
import { z } from 'zod';
import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { AdminGuard, CsrfGuard, SessionAuthGuard } from '../auth/auth.guards.js';
import { ArtifactsService } from './artifacts.service.js';
import { ArtifactFileError } from './artifact-files.js';

function identifier(value: string): string {
  const parsed = z.uuid().safeParse(value);
  if (!parsed.success) throw new BadRequestException({ code: 'INVALID_REQUEST' });
  return parsed.data;
}
function mapError(error: unknown): never {
  if (
    error instanceof ArtifactFileError ||
    error instanceof ArtifactIntegrityError ||
    error instanceof ElfInspectionError ||
    error instanceof LocalStoragePathError
  ) {
    throw new ConflictException({ code: error.code });
  }
  if (typeof error === 'object' && error !== null && 'code' in error) {
    if (error.code === '23505')
      throw new ConflictException({ code: 'ARTIFACT_VERSION_ALREADY_EXISTS' });
    if (error.code === 'ENOENT' || error.code === 'ENOTDIR')
      throw new ConflictException({ code: 'ARTIFACT_FILE_NOT_FOUND' });
    if (error.code === 'EACCES')
      throw new ConflictException({ code: 'ARTIFACT_FILE_ACCESS_DENIED' });
  }
  throw error;
}
@Controller('artifacts')
export class ArtifactsController {
  constructor(private readonly artifacts: ArtifactsService) {}
  @Delete(':id')
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  async delete(@Req() request: AuthenticatedFastifyRequest, @Param('id') id: string) {
    return this.artifacts.delete(requireAuthContext(request).user, identifier(id));
  }
  @Post('files/upload')
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  async uploadFile(
    @Req() request: AuthenticatedFastifyRequest,
    @Res({ passthrough: true }) reply: FastifyReply,
    @Body() body: unknown,
  ) {
    const encoded = request.headers['x-artifact-filename'];
    let name: string;
    try {
      if (typeof encoded !== 'string' || !(body instanceof Readable)) throw new Error();
      name = decodeURIComponent(encoded);
    } catch {
      throw new BadRequestException({ code: 'INVALID_REQUEST' });
    }
    const cancellation = new AbortController();
    const cancel = () => cancellation.abort();
    request.raw.once('aborted', cancel);
    reply.raw.once('close', cancel);
    if (request.raw.aborted) cancellation.abort();
    try {
      return await this.artifacts.uploadFile(name, body as Readable, cancellation.signal);
    } catch (error) {
      mapError(error);
    } finally {
      request.raw.off('aborted', cancel);
      reply.raw.off('close', cancel);
    }
  }
  @Get('files')
  @UseGuards(SessionAuthGuard, AdminGuard)
  async files() {
    try {
      return await this.artifacts.listFiles();
    } catch (error) {
      mapError(error);
    }
  }
  @Post('files/prepare')
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  async prepareFile(@Body() body: unknown) {
    const parsed = artifactFileCandidateSchema.safeParse(body);
    if (!parsed.success) throw new BadRequestException({ code: 'INVALID_REQUEST' });
    try {
      return await this.artifacts.prepareFile(parsed.data);
    } catch (error) {
      mapError(error);
    }
  }
  @Post('files/register')
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  async registerFile(@Req() request: AuthenticatedFastifyRequest, @Body() body: unknown) {
    const parsed = registerArtifactFileRequestSchema.safeParse(body);
    if (!parsed.success) throw new BadRequestException({ code: 'INVALID_REQUEST' });
    try {
      return await this.artifacts.registerFile(requireAuthContext(request).user, parsed.data);
    } catch (error) {
      mapError(error);
    }
  }
  @Get()
  @UseGuards(SessionAuthGuard)
  async list(@Req() request: AuthenticatedFastifyRequest) {
    return { artifacts: await this.artifacts.list(requireAuthContext(request).user) };
  }
  @Post()
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  async register(@Req() request: AuthenticatedFastifyRequest, @Body() body: unknown) {
    const parsed = registerArtifactRequestSchema.safeParse(body);
    if (!parsed.success) throw new BadRequestException({ code: 'INVALID_REQUEST' });
    try {
      return await this.artifacts.register(requireAuthContext(request).user, parsed.data);
    } catch (error) {
      mapError(error);
    }
  }
  @Post(':id/verify')
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  async verify(@Req() request: AuthenticatedFastifyRequest, @Param('id') id: string) {
    try {
      return await this.artifacts.verify(requireAuthContext(request).user, identifier(id));
    } catch (error) {
      mapError(error);
    }
  }
  @Post(':id/activate')
  @UseGuards(SessionAuthGuard, CsrfGuard, AdminGuard)
  async activate(@Req() request: AuthenticatedFastifyRequest, @Param('id') id: string) {
    try {
      return await this.artifacts.verify(requireAuthContext(request).user, identifier(id), true);
    } catch (error) {
      mapError(error);
    }
  }
}
