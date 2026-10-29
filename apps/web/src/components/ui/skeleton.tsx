// Adapted from satnaing/shadcn-admin (MIT); see third-party/shadcn-admin-LICENSE.
import { cn } from '../../lib/utils';

function Skeleton({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="skeleton"
      className={cn('animate-pulse rounded-md bg-accent', className)}
      {...props}
    />
  );
}

export { Skeleton };
