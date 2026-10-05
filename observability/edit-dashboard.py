# Small helper for editing the provisioned dashboard while keeping its
# one-panel-per-line layout. Each series gets a fixed categorical color by
# position, so an entity keeps its color across panels.
import json, sys
PATH = 'observability/grafana/dashboards/seatrush.json'
C = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300']

def load():
    return json.load(open(PATH))

def set_series(panel, items):
    panel['targets'] = [{'refId': chr(65 + i), 'expr': q, 'legendFormat': n} for i, (n, q) in enumerate(items)]
    panel['fieldConfig']['overrides'] = [{'matcher': {'id': 'byFrameRefID', 'options': chr(65 + i)},
        'properties': [{'id': 'color', 'value': {'mode': 'fixed', 'fixedColor': C[i]}}]} for i in range(len(items))]
    if 'legend' in panel.get('options', {}): panel['options']['legend']['showLegend'] = len(items) > 1

def save(d):
    head = {k: v for k, v in d.items() if k != 'panels'}
    body = json.dumps(head)[:-1] + ',\n "panels": [\n' + ',\n'.join('  ' + json.dumps(p) for p in d['panels']) + '\n ]\n}\n'
    open(PATH, 'w').write(body)
