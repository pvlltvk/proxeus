## Proxeus, or how you ended up with four metrics systems

### One Prometheus

You start with a Prometheus. Scrape config, a few alerts, Grafana in front. Everything is in one place and every
dashboard just works. This is the good part.

### Then it doesn't fit

Retention is the first wall. Prometheus keeps a few weeks on local disk, and someone asks for a year-over-year
comparison. So you add long-term storage — say **Thanos**, with a sidecar shipping blocks to object storage and a
Querier fanning out across them.

Now you have two places data lives. Grafana gets a second datasource. Dashboards start to care *which* one they point
at: recent data here, historical data there. Panels that span the boundary need editing, and the boundary moves as
retention rolls.

### Then a second system arrives

A team with high-cardinality metrics finds Prometheus too slow and stands up **VictoriaMetrics**, which handles their
churn better and costs less. Nobody is wrong about this — the workloads genuinely differ, and both tools are good at
what they do.

You now have Thanos and VictoriaMetrics side by side. Grafana has datasources for each. And the questions that matter
most to you are exactly the ones that span both:

> What is the global error rate?

You cannot ask it. Not in one query, not in one panel, not in one alert. `sum(rate(errors[5m]))` runs against *one*
datasource. Grafana's mixed-datasource mode will draw two series next to each other, but it will not add them together,
and it will not let you alert on the sum.

### The usual workarounds, and why they hurt

**A global aggregation layer** — scrape everything into one more Prometheus. This drops granularity by design, becomes
the majority of load on everything it scrapes, and gives you a second set of alerting rules to maintain, subtly
different from the first.

**Federating in Grafana** — duplicate every panel per datasource and eyeball the sum. Works until someone needs an
alert, which is precisely when arithmetic matters.

**Pick one system and migrate** — reasonable, and you should, eventually. But migration is not a weekend. You need both
systems serving queries for months, and during that window every dashboard is wrong.

### Proxeus

Proxeus puts one PromQL endpoint in front of all of them.

Grafana gets a single datasource. `sum(rate(errors[5m]))` scatters to Thanos and VictoriaMetrics in parallel, and the
results come back as one series set — so global aggregation is just a query again, and alerts can be written against
it. Backends stay exactly as they are; nothing is scraped twice and no sidecar is installed.

Because backends can overlap — the same target scraped into two systems during a migration — proxeus deduplicates
across them deterministically: a series present in both collapses to one, and the winner is decided by configuration
order, not by whichever backend answered first. Aggregations are pushed down to each backend rather than dragging raw
series across the network.

And when the migration finally finishes, you delete a `server_group` from the config. No dashboard changes.

### What it does not do

Proxeus is a read path. It does not store anything, does not scrape anything, and cannot fix data a backend does not
have. If a backend is down, you decide per configuration whether that fails the query or returns a partial answer with
a warning — proxeus will not silently pretend the data was complete.
