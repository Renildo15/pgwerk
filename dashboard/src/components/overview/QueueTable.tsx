import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { Skeleton } from '@/components/ui/skeleton'
import { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider } from '@/components/ui/tooltip'
import { statusColor } from '@/components/StatusBadge'
import type { QueueStats, UnroutableEntry } from '@/types'
import { AlertTriangle } from 'lucide-react'
import { Link } from 'react-router-dom'

const STATUS_COLS = ['queued', 'active', 'waiting', 'failed', 'complete', 'aborted'] as const

interface QueueTableProps {
  queues: QueueStats[]
  /** Runnable jobs no live worker would claim; flagged next to their queue. */
  unroutable?: UnroutableEntry[]
}

function UnroutableFlag({ entries }: { entries: UnroutableEntry[] }) {
  const total = entries.reduce((sum, e) => sum + e.count, 0)
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Link
          to="/functions"
          className="inline-flex items-center gap-1 rounded border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 font-mono text-[10px] text-amber-500"
        >
          <AlertTriangle className="h-3 w-3" />
          {total} unroutable
        </Link>
      </TooltipTrigger>
      <TooltipContent>
        <p className="mb-1 text-xs">No live worker would claim:</p>
        {entries.map(e => (
          <p key={e.function} className="font-mono text-xs">
            {e.function} × {e.count}
          </p>
        ))}
      </TooltipContent>
    </Tooltip>
  )
}

export function QueueTable({ queues, unroutable = [] }: QueueTableProps) {
  const unroutableByQueue = new Map<string, UnroutableEntry[]>()
  for (const entry of unroutable) {
    unroutableByQueue.set(entry.queue, [...(unroutableByQueue.get(entry.queue) ?? []), entry])
  }

  return (
    <TooltipProvider>
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="w-40">Queue</TableHead>
          {STATUS_COLS.map(s => (
            <TableHead key={s} className="w-24 text-right font-mono capitalize">
              {s}
            </TableHead>
          ))}
          <TableHead>Distribution</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {queues.map(q => {
          const total = STATUS_COLS.reduce((sum, s) => sum + (q[s] ?? 0), 0)
          return (
            <TableRow key={q.queue}>
              <TableCell className="font-mono text-sm font-medium">
                <div className="flex flex-wrap items-center gap-2">
                  {q.queue}
                  {unroutableByQueue.has(q.queue) && <UnroutableFlag entries={unroutableByQueue.get(q.queue)!} />}
                </div>
              </TableCell>
              {STATUS_COLS.map(s => (
                <TableCell key={s} className="text-right font-mono text-sm tabular-nums text-muted-foreground">
                  {q[s] ?? 0}
                </TableCell>
              ))}
              <TableCell>
                {total > 0 ? (
                  <div className="flex h-1.5 w-full overflow-hidden rounded-full bg-muted">
                    {STATUS_COLS.filter(s => (q[s] ?? 0) > 0).map(s => (
                      <div
                        key={s}
                        className={`h-full ${statusColor(s)} opacity-80`}
                        style={{ width: `${((q[s] ?? 0) / total) * 100}%` }}
                      />
                    ))}
                  </div>
                ) : (
                  <div className="h-1.5 w-full rounded-full bg-muted" />
                )}
              </TableCell>
            </TableRow>
          )
        })}
        {queues.length === 0 && (
          <TableRow>
            <TableCell colSpan={8} className="py-8 text-center text-sm text-muted-foreground">
              No queues found
            </TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
    </TooltipProvider>
  )
}

export function QueueTableSkeleton() {
  return (
    <div className="space-y-3 p-4">
      {Array.from({ length: 3 }).map((_, i) => (
        <Skeleton key={i} className="h-8 w-full" />
      ))}
    </div>
  )
}
