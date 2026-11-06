import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table'
import { api } from '@/lib/api'
import { relativeTime, shortFn } from '@/lib/utils'
import { useQuery } from '@tanstack/react-query'
import { AlertTriangle, RefreshCw, Search } from 'lucide-react'
import { useMemo, useState } from 'react'

function TableSkeleton() {
  return (
    <div className="space-y-2 p-4">
      {Array.from({ length: 3 }).map((_, i) => (
        <Skeleton key={i} className="h-10 w-full" />
      ))}
    </div>
  )
}

export function FunctionsPage() {
  const [search, setSearch] = useState('')

  const functions = useQuery({
    queryKey: ['functions'],
    queryFn: api.listFunctions,
    refetchInterval: 30_000,
  })
  const unroutable = useQuery({
    queryKey: ['functions', 'unroutable'],
    queryFn: api.listUnroutable,
    refetchInterval: 10_000,
  })

  const visible = useMemo(() => {
    const rows = functions.data ?? []
    if (!search) return rows
    const q = search.toLowerCase()
    return rows.filter(f => f.name.toLowerCase().includes(q))
  }, [functions.data, search])

  const stuck = unroutable.data ?? []
  const stuckTotal = stuck.reduce((sum, r) => sum + r.count, 0)
  const isFetching = functions.isFetching || unroutable.isFetching

  return (
    <div className="flex flex-col">
      <div className="flex h-12 items-center justify-between">
        <h1 className="text-sm font-semibold">Functions</h1>
        <Button
          variant="ghost"
          size="icon"
          className="h-8 w-8"
          onClick={() => { functions.refetch(); unroutable.refetch() }}
          disabled={isFetching}
        >
          <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? 'animate-spin' : ''}`} />
        </Button>
      </div>

      <div className="flex-1 space-y-6 py-6">
        {/* Jobs nobody can run */}
        <section className="space-y-2">
          <div className="flex items-baseline gap-2">
            <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Unroutable jobs
            </h2>
            <span className="text-xs text-muted-foreground">
              Runnable jobs that no live worker would claim
            </span>
          </div>
          <Card className="overflow-hidden">
            {unroutable.isLoading ? <TableSkeleton /> : stuck.length === 0 ? (
              <p className="py-6 text-center text-sm text-muted-foreground">
                Every runnable job has a live worker that can claim it.
              </p>
            ) : (
              <>
                <div className="flex items-center gap-2 border-b border-border bg-amber-500/5 px-4 py-2 text-xs text-amber-500">
                  <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                  {stuckTotal} job{stuckTotal === 1 ? '' : 's'} waiting with no worker that can run them:
                  no worker consumes the queue, or none declared the function.
                </div>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-40">Queue</TableHead>
                      <TableHead>Function</TableHead>
                      <TableHead className="w-24 text-right">Jobs</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {stuck.map(r => (
                      <TableRow key={`${r.queue}\u0000${r.function}`}>
                        <TableCell className="font-mono text-xs">{r.queue}</TableCell>
                        <TableCell className="font-mono text-xs text-muted-foreground" title={r.function}>
                          {r.function}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs tabular-nums text-amber-500">
                          {r.count}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </>
            )}
          </Card>
        </section>

        {/* Catalog */}
        <section className="space-y-2">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-baseline gap-2">
              <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                Registered functions
              </h2>
              <span className="text-xs text-muted-foreground">
                Declared by workers started with <code className="font-mono">functions=</code>
              </span>
            </div>
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
              <input
                type="text"
                placeholder="Search functions…"
                value={search}
                onChange={e => setSearch(e.target.value)}
                className="h-8 w-64 rounded-md border border-input bg-background pl-8 pr-3 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </div>
          </div>
          <Card className="overflow-hidden">
            {functions.isLoading ? <TableSkeleton /> : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Function</TableHead>
                    <TableHead className="w-48">Languages</TableHead>
                    <TableHead className="w-40">Last registered</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {visible.map(f => (
                    <TableRow key={f.name}>
                      <TableCell className="font-mono text-xs font-medium" title={f.name}>
                        {shortFn(f.name)}
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-1">
                          {f.languages.map(lang => (
                            <span
                              key={lang}
                              className="rounded border border-border px-1.5 py-0.5 font-mono text-[10px] text-muted-foreground"
                            >
                              {lang}
                            </span>
                          ))}
                        </div>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {relativeTime(f.last_registered_at)}
                      </TableCell>
                    </TableRow>
                  ))}
                  {visible.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={3} className="py-8 text-center text-sm text-muted-foreground">
                        {search
                          ? 'No functions match'
                          : 'No worker has declared its functions. Workers without functions= claim every function on their queues.'}
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            )}
          </Card>
        </section>
      </div>
    </div>
  )
}
