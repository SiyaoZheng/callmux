import { useEffect, useState } from 'react'
import { ChevronDown, ChevronRight, Folder, Globe2 } from 'lucide-react'
import { useStore } from '@/store'
import { Card } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { DetailItem } from '@/components/shared/bits'
import { formatDateTime, formatNum } from '@/lib/format'
import type { ResearchProvider, UrlTreeNode } from '@/types'

function ProviderBadge({ provider }: { provider: ResearchProvider }) {
  return <Badge variant="outline">{provider === 'exa' ? 'Exa' : '搜狗'}</Badge>
}

function TreeRow({ node }: { node: UrlTreeNode }) {
  const [open, setOpen] = useState(node.kind === 'domain')
  const hasChildren = node.children.length > 0
  const Icon = node.kind === 'domain' ? Globe2 : Folder
  return (
    <div>
      <button
        type="button"
        onClick={() => hasChildren && setOpen((value) => !value)}
        className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-muted/60"
        style={{ paddingLeft: `${8 + node.depth * 18}px` }}
        aria-expanded={hasChildren ? open : undefined}
      >
        {hasChildren ? (
          open ? <ChevronDown className="size-3.5 shrink-0 text-muted-foreground" /> : <ChevronRight className="size-3.5 shrink-0 text-muted-foreground" />
        ) : (
          <span className="w-3.5 shrink-0" />
        )}
        <Icon className="size-4 shrink-0 text-sky-500" />
        <span className="min-w-0 flex-1 truncate font-mono text-[13px]" title={node.prefix}>{node.segment || '/'}</span>
        <span className="shrink-0 text-xs text-muted-foreground">{formatNum(node.pages)} pages</span>
        {node.discoveries > 0 && <Badge variant="secondary">{formatNum(node.discoveries)} found</Badge>}
        {node.fetches > 0 && <Badge variant="outline">{formatNum(node.fetches)} fetched</Badge>}
        {node.projects.length > 0 && (
          <Badge variant="secondary" title={node.projects.map((project) => project.path).join('\n')}>
            {node.projects.map((project) => project.name).join(', ')}
          </Badge>
        )}
        {node.signatures.length > 0 && <Badge variant="outline">{formatNum(node.signatures.length)} Codex</Badge>}
      </button>
      {open && hasChildren && (
        <div>{node.children.map((child) => <TreeRow key={child.prefix} node={child} />)}</div>
      )}
    </div>
  )
}

export function ResearchIndexView() {
  const index = useStore((s) => s.researchIndex)
  const load = useStore((s) => s.loadResearchIndex)
  const filters = useStore((s) => s.researchFilters)
  const setFilters = useStore((s) => s.setResearchFilters)

  useEffect(() => {
    void load()
  }, [load])

  if (index?.enabled === false) {
    return <Card className="p-4 text-sm text-muted-foreground">SQLite event store is disabled.</Card>
  }

  const totals = index?.totals
  return (
    <div className="space-y-4">
      <Card className="space-y-3 p-4">
        <div>
          <h2 className="text-lg font-semibold">URL Tree Index</h2>
          <p className="text-sm text-muted-foreground">
            Domains are roots; URL path segments form the branches. Discovery and fetch counts remain distinct.
          </p>
        </div>
        <div className="grid gap-2 sm:grid-cols-[minmax(160px,0.7fr)_minmax(220px,1fr)_auto_auto]">
          <Input
            value={filters.project}
            onChange={(event) => setFilters({ project: event.target.value })}
            placeholder="Project name or path"
            aria-label="Filter by project name or path"
          />
          <Input
            value={filters.signature}
            onChange={(event) => setFilters({ signature: event.target.value })}
            placeholder="Codex/thread-id"
            aria-label="Filter by Codex thread signature"
          />
          <Button type="button" variant="secondary" onClick={() => void load()}>Apply</Button>
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              setFilters({ project: '', signature: '' })
              queueMicrotask(() => void load())
            }}
          >
            Clear
          </Button>
        </div>
        {totals && (
          <div className="grid grid-cols-[repeat(auto-fit,minmax(120px,1fr))] gap-2.5">
            <DetailItem label="Domains" value={formatNum(totals.domains)} />
            <DetailItem label="Pages" value={formatNum(totals.pages)} />
            <DetailItem label="Discovered" value={formatNum(totals.discoveries)} />
            <DetailItem label="Fetched" value={formatNum(totals.fetches)} />
            <DetailItem label="Queries" value={formatNum(totals.queries)} />
            <DetailItem label="Unique queries" value={formatNum(totals.uniqueQueries)} />
          </div>
        )}
      </Card>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.6fr)_minmax(300px,0.8fr)]">
        <Card className="overflow-hidden p-0">
          <h2 className="px-4 pt-4 text-lg font-semibold">URL hierarchy</h2>
          <div className="mt-2 max-h-[680px] overflow-auto border-t border-border p-2">
            {!index?.urlTree?.length ? (
              <div className="p-3 text-sm text-muted-foreground">No fetched or discovered URLs recorded yet.</div>
            ) : (
              index.urlTree.map((node) => <TreeRow key={node.prefix} node={node} />)
            )}
          </div>
        </Card>

        <div className="space-y-4">
          <Card className="p-4">
            <h2 className="text-lg font-semibold">High-frequency terms</h2>
            <div className="mt-3 flex flex-wrap gap-2">
              {index?.topTerms?.slice(0, 24).map((row) => (
                <div key={`${row.provider}:${row.term}`} className="flex items-center gap-1.5 rounded-md border border-border px-2 py-1 text-xs">
                  <ProviderBadge provider={row.provider} />
                  <span>{row.term}</span>
                  <span className="text-muted-foreground">×{formatNum(row.uses)}</span>
                </div>
              ))}
            </div>
          </Card>

          <Card className="overflow-hidden p-0">
            <h2 className="px-4 pt-4 text-lg font-semibold">Top queries</h2>
            <div className="mt-2 max-h-[420px] overflow-auto">
              {index?.topQueries?.slice(0, 30).map((row) => (
                <div key={`${row.provider}:${row.query}`} className="border-t border-border px-4 py-2.5 text-sm">
                  <div className="flex items-start gap-2">
                    <ProviderBadge provider={row.provider} />
                    <span className="min-w-0 flex-1 break-words">{row.query}</span>
                    <span className="shrink-0 text-muted-foreground">×{formatNum(row.calls)}</span>
                  </div>
                  <div className="mt-1 text-xs text-muted-foreground">Last: {formatDateTime(row.lastSeenAt)}</div>
                  {(row.projects.length > 0 || row.signatures.length > 0) && (
                    <div className="mt-1 flex flex-wrap gap-1">
                      {row.projects.map((project) => <Badge key={project.path} variant="secondary" title={project.path}>{project.name}</Badge>)}
                      {row.signatures.map((signature) => <Badge key={signature} variant="outline" title={signature}>{signature.replace(/^Codex\//, 'Codex/').slice(0, 22)}{signature.length > 22 ? '…' : ''}</Badge>)}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </Card>
        </div>
      </div>

      <Card className="overflow-hidden p-0">
        <h2 className="px-4 pt-4 text-lg font-semibold">Indexed pages</h2>
        <div className="mt-2 overflow-x-auto">
          <table className="w-full text-[13px]">
            <thead>
              <tr className="border-t border-border text-left text-muted-foreground">
                <th className="p-2 font-semibold">Provider</th>
                <th className="p-2 font-semibold">Page</th>
                <th className="p-2 font-semibold">Found</th>
                <th className="p-2 font-semibold">Fetched</th>
                <th className="p-2 font-semibold">Project / Codex</th>
                <th className="p-2 font-semibold">Last seen</th>
              </tr>
            </thead>
            <tbody>
              {index?.pages?.map((page) => (
                <tr key={`${page.provider}:${page.canonicalUrl}`} className="border-t border-border">
                  <td className="p-2"><ProviderBadge provider={page.provider} /></td>
                  <td className="max-w-[680px] p-2">
                    <a href={page.canonicalUrl} target="_blank" rel="noreferrer" className="font-medium text-sky-600 hover:underline">
                      {page.title || page.path || page.domain}
                    </a>
                    <div className="truncate font-mono text-xs text-muted-foreground" title={page.canonicalUrl}>{page.canonicalUrl}</div>
                  </td>
                  <td className="p-2">{formatNum(page.discoveries)}</td>
                  <td className="p-2">{formatNum(page.fetches)}</td>
                  <td className="p-2">
                    <div className="flex max-w-[280px] flex-wrap gap-1">
                      {page.projects.map((project) => <Badge key={project.path} variant="secondary" title={project.path}>{project.name}</Badge>)}
                      {page.signatures.map((signature) => <Badge key={signature} variant="outline" title={signature}>Codex/{signature.split('/').at(-1)?.slice(0, 8)}</Badge>)}
                    </div>
                  </td>
                  <td className="p-2 text-muted-foreground">{formatDateTime(page.lastSeenAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  )
}
