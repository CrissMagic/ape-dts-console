import { Controller, Get, Req, UseGuards } from '@nestjs/common';
import { requireAuthContext, type AuthenticatedFastifyRequest } from '../auth/auth-http.js';
import { SessionAuthGuard } from '../auth/auth.guards.js';
import { TaskProgressService } from './task-progress.service.js';

@Controller('tasks/progress')
export class TaskProgressController {
  constructor(private readonly progress: TaskProgressService) {}
  @Get()
  @UseGuards(SessionAuthGuard)
  list(@Req() request: AuthenticatedFastifyRequest) {
    return this.progress.list(requireAuthContext(request).user);
  }
}
