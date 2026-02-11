// NovusDB Interactive Terminal Demo
// Auto-typing terminal simulation with real queries and results

const demoSteps = [
  // ── Phase 1: Nested SQL inserts with dot-notation ──
  {
    type: 'comment',
    text: '── SQL inserts with nested fields (dot-notation) ──'
  },
  {
    type: 'command',
    text: `INSERT INTO employees VALUES (
    name="Alice", age=32, role="lead",
    address.city="Paris", address.zip="75001", address.country="France",
    salary=72000, department="engineering")`,
    result: '  OK — 1 row(s) affected, last ID: 1'
  },
  {
    type: 'command',
    text: `INSERT INTO employees VALUES (
    name="Bob", age=27, role="dev",
    address.city="Lyon", address.zip="69001", address.country="France",
    salary=48000, department="engineering")`,
    result: '  OK — 1 row(s) affected, last ID: 2'
  },
  {
    type: 'command',
    text: `INSERT INTO employees VALUES (
    name="Charlie", age=45, role="CTO",
    address.city="Berlin", address.zip="10115", address.country="Germany",
    salary=120000, department="executive")`,
    result: '  OK — 1 row(s) affected, last ID: 3'
  },
  {
    type: 'command',
    text: `INSERT INTO employees VALUES (
    name="Diana", age=29, role="dev",
    address.city="Bordeaux", address.zip="33000", address.country="France",
    salary=52000, department="engineering")`,
    result: '  OK — 1 row(s) affected, last ID: 4'
  },
  { type: 'pause', duration: 400 },

  // ── Phase 2: JSON inserts with deep nesting ──
  {
    type: 'comment',
    text: '── Native JSON insert with arrays, nested objects ──'
  },
  {
    type: 'command',
    text: `INSERT JSON INTO employees {
    "name": "Eve",
    "age": 38,
    "role": "architect",
    "address": {
      "city": "Paris",
      "zip": "75011",
      "country": "France",
      "geo": {"lat": 48.8566, "lng": 2.3522}
    },
    "salary": 95000,
    "department": "engineering",
    "skills": ["Go", "Rust", "PostgreSQL"],
    "certifications": [
      {"name": "AWS Solutions Architect", "year": 2024},
      {"name": "CKA Kubernetes", "year": 2025}
    ]
  }`,
    result: '  OK — 1 row(s) affected, last ID: 5'
  },
  {
    type: 'command',
    text: `INSERT JSON INTO employees {
    "name": "Frank",
    "age": 33,
    "role": "devops",
    "address": {
      "city": "Toulouse",
      "zip": "31000",
      "country": "France",
      "geo": {"lat": 43.6047, "lng": 1.4442}
    },
    "salary": 65000,
    "department": "infrastructure",
    "skills": ["Docker", "Kubernetes", "Terraform"],
    "certifications": [
      {"name": "CKA Kubernetes", "year": 2025}
    ]
  }`,
    result: '  OK — 1 row(s) affected, last ID: 6'
  },
  { type: 'pause', duration: 400 },

  // ── Phase 3: Orders with nested payment info ──
  {
    type: 'comment',
    text: '── Orders with nested payment & shipping ──'
  },
  {
    type: 'command',
    text: `INSERT INTO orders VALUES (
    employee="Alice", product="NovusDB Pro", total=299,
    payment.method="card", payment.status="paid",
    shipping.city="Paris", shipping.express=true)`,
    result: '  OK — 1 row(s) affected, last ID: 1'
  },
  {
    type: 'command',
    text: `INSERT INTO orders VALUES (
    employee="Charlie", product="NovusDB Enterprise", total=2499,
    payment.method="invoice", payment.status="pending",
    shipping.city="Berlin", shipping.express=false)`,
    result: '  OK — 1 row(s) affected, last ID: 2'
  },
  {
    type: 'command',
    text: `INSERT INTO orders VALUES (
    employee="Bob", product="NovusDB Team", total=899,
    payment.method="card", payment.status="paid",
    shipping.city="Lyon", shipping.express=true)`,
    result: '  OK — 1 row(s) affected, last ID: 3'
  },
  {
    type: 'command',
    text: `INSERT INTO orders VALUES (
    employee="Eve", product="Support Premium", total=450,
    payment.method="transfer", payment.status="paid",
    shipping.city="Paris", shipping.express=false)`,
    result: '  OK — 1 row(s) affected, last ID: 4'
  },
  { type: 'pause', duration: 400 },

  // ── Phase 4: Indexes ──
  {
    type: 'comment',
    text: '── Create indexes on nested fields ──'
  },
  {
    type: 'command',
    text: 'CREATE INDEX ON employees(address.city)',
    result: '  Index created on employees.address.city'
  },
  {
    type: 'command',
    text: 'CREATE INDEX ON employees(department)',
    result: '  Index created on employees.department'
  },
  { type: 'pause', duration: 600 },

  // ── Phase 5: Queries on nested fields ──
  {
    type: 'comment',
    text: '── Query nested fields with dot-notation ──'
  },
  {
    type: 'command',
    text: `SELECT name, role, address.city, address.country
  FROM employees
  WHERE address.country = "France"
  ORDER BY salary DESC`,
    result: `  [#1] name="Eve"     role="architect"  address.city="Paris"     address.country="France"
  [#2] name="Alice"   role="lead"       address.city="Paris"     address.country="France"
  [#3] name="Frank"   role="devops"     address.city="Toulouse"  address.country="France"
  [#4] name="Diana"   role="dev"        address.city="Bordeaux"  address.country="France"
  [#5] name="Bob"     role="dev"        address.city="Lyon"      address.country="France"
  --- 5 document(s)`
  },
  { type: 'pause', duration: 800 },
  {
    type: 'comment',
    text: '── Deep nested query: geo coordinates ──'
  },
  {
    type: 'command',
    text: `SELECT name, address.city, address.geo.lat, address.geo.lng
  FROM employees
  WHERE address.geo.lat > 45`,
    result: `  [#1] name="Eve"    address.city="Paris"  address.geo.lat=48.8566  address.geo.lng=2.3522
  --- 1 document(s)`
  },
  { type: 'pause', duration: 800 },

  // ── Phase 6: JOIN + aggregation on nested ──
  {
    type: 'comment',
    text: '── JOIN with nested fields + aggregation ──'
  },
  {
    type: 'command',
    text: `SELECT e.name, e.address.city, SUM(o.total) AS revenue,
    COUNT(*) AS num_orders, e.department
  FROM employees e
  JOIN orders o ON e.name = o.employee
  GROUP BY e.name
  ORDER BY revenue DESC`,
    result: `  [#1] name="Charlie"  address.city="Berlin"  revenue=2499  num_orders=1  department="executive"
  [#2] name="Bob"      address.city="Lyon"    revenue=899   num_orders=1  department="engineering"
  [#3] name="Eve"      address.city="Paris"   revenue=450   num_orders=1  department="engineering"
  [#4] name="Alice"    address.city="Paris"   revenue=299   num_orders=1  department="engineering"
  --- 4 document(s)`
  },
  { type: 'pause', duration: 800 },

  // ── Phase 7: CASE WHEN on nested ──
  {
    type: 'comment',
    text: '── CASE WHEN on nested + computed fields ──'
  },
  {
    type: 'command',
    text: `SELECT name, salary, address.country,
    CASE
      WHEN salary > 100000 THEN "executive"
      WHEN salary > 70000  THEN "senior"
      WHEN salary > 50000  THEN "mid"
      ELSE "junior"
    END AS tier,
    CASE
      WHEN address.country = "France" THEN "🇫🇷 EU"
      WHEN address.country = "Germany" THEN "🇩🇪 EU"
      ELSE "other"
    END AS region
  FROM employees
  ORDER BY salary DESC`,
    result: `  [#1] name="Charlie"  salary=120000  address.country="Germany"  tier="executive"  region="🇩🇪 EU"
  [#2] name="Eve"      salary=95000   address.country="France"   tier="senior"     region="🇫🇷 EU"
  [#3] name="Alice"    salary=72000   address.country="France"   tier="senior"     region="🇫🇷 EU"
  [#4] name="Frank"    salary=65000   address.country="France"   tier="mid"        region="🇫🇷 EU"
  [#5] name="Diana"    salary=52000   address.country="France"   tier="mid"        region="🇫🇷 EU"
  [#6] name="Bob"      salary=48000   address.country="France"   tier="junior"     region="🇫🇷 EU"
  --- 6 document(s)`
  },
  { type: 'pause', duration: 800 },

  // ── Phase 8: Subquery on nested fields ──
  {
    type: 'comment',
    text: '── Subquery: above-average salary in their city ──'
  },
  {
    type: 'command',
    text: `SELECT name, salary, address.city
  FROM employees e
  WHERE salary > (
    SELECT AVG(salary) FROM employees
    WHERE address.city = e.address.city
  )`,
    result: `  [#1] name="Eve"    salary=95000  address.city="Paris"
  [#2] name="Diana"  salary=52000  address.city="Bordeaux"
  --- 2 document(s)`
  },
  { type: 'pause', duration: 800 },

  // ── Phase 9: EXPLAIN with hints on nested index ──
  {
    type: 'comment',
    text: '── EXPLAIN with hints on nested index ──'
  },
  {
    type: 'command',
    text: `EXPLAIN SELECT /*+ INDEX_LOOKUP(orders, employee) */
    e.name, e.address.city, o.product, o.payment.status
  FROM employees e
  JOIN orders o ON e.name = o.employee
  WHERE e.address.city = "Paris"
    AND o.payment.status = "paid"`,
    result: `  Plan:
  ├─ Scan employees (index: address.city = "Paris") → est. 2 rows
  ├─ Join orders (strategy: INDEX_LOOKUP on employee)
  │   └─ Hint applied: INDEX_LOOKUP(orders, employee)
  ├─ Filter: o.payment.status = "paid"
  ├─ Estimated cost: 0.08
  └─ Selectivity: 0.25

  [#1] name="Alice"  address.city="Paris"  product="NovusDB Pro"    payment.status="paid"
  [#2] name="Eve"    address.city="Paris"  product="Support Premium" payment.status="paid"
  --- 2 document(s)`
  },
  { type: 'pause', duration: 800 },

  // ── Phase 10: LEFT JOIN + UNION ──
  {
    type: 'comment',
    text: '── LEFT JOIN: employees without orders ──'
  },
  {
    type: 'command',
    text: `SELECT e.name, e.address.city, e.role, o.product
  FROM employees e
  LEFT JOIN orders o ON e.name = o.employee
  WHERE o.product IS NULL`,
    result: `  [#1] name="Diana"  address.city="Bordeaux"   role="dev"     product=null
  [#2] name="Frank"  address.city="Toulouse"   role="devops"  product=null
  --- 2 document(s)`
  },
  { type: 'pause', duration: 600 },

  // ── Phase 11: Query arrays and certifications ──
  {
    type: 'comment',
    text: '── Query arrays and nested array objects ──'
  },
  {
    type: 'command',
    text: 'SELECT name, skills, certifications FROM employees WHERE skills IS NOT NULL',
    result: `  [#1] name="Eve"    skills=["Go","Rust","PostgreSQL"]      certifications=[{"name":"AWS Solutions Architect","year":2024},{"name":"CKA Kubernetes","year":2025}]
  [#2] name="Frank"  skills=["Docker","Kubernetes","Terraform"]  certifications=[{"name":"CKA Kubernetes","year":2025}]
  --- 2 document(s)`
  },
  { type: 'pause', duration: 600 },

  // ── Phase 12: Schema + cache ──
  {
    type: 'command',
    text: '.schema',
    result: `  employees (6 documents)
    ├─ name            string  (6/6 = 100%)
    ├─ age             int64   (6/6 = 100%)
    ├─ role            string  (6/6 = 100%)
    ├─ address         object  (6/6 = 100%)
    │   ├─ city        string  (6/6 = 100%)
    │   ├─ zip         string  (6/6 = 100%)
    │   ├─ country     string  (6/6 = 100%)
    │   └─ geo         object  (2/6 = 33%)
    ├─ salary          int64   (6/6 = 100%)
    ├─ department      string  (6/6 = 100%)
    ├─ skills          array   (2/6 = 33%)
    └─ certifications  array   (2/6 = 33%)
  orders (4 documents)
    ├─ employee        string  (4/4 = 100%)
    ├─ product         string  (4/4 = 100%)
    ├─ total           int64   (4/4 = 100%)
    ├─ payment         object  (4/4 = 100%)
    │   ├─ method      string  (4/4 = 100%)
    │   └─ status      string  (4/4 = 100%)
    └─ shipping        object  (4/4 = 100%)
        ├─ city        string  (4/4 = 100%)
        └─ express     bool    (4/4 = 100%)`
  },
  { type: 'pause', duration: 600 },
  {
    type: 'command',
    text: '.cache',
    result: `  LRU Page Cache:
    Capacity : 256 pages (1024 KB)
    Size     : 18 pages
    Hits     : 1243
    Misses   : 31
    Hit rate : 97.6%`
  },
  {
    type: 'comment',
    text: '── Done! Nested documents, dot-notation, JSON, JOINs, hints — all in one. ──'
  }
];

class TerminalDemo {
  constructor(container) {
    this.container = container;
    this.outputEl = container.querySelector('.demo-output');
    this.isRunning = false;
    this.isPaused = false;
    this.currentStep = 0;
    this.typingSpeed = 22;
    this.aborted = false;
  }

  async start() {
    if (this.isRunning) return;
    this.isRunning = true;
    this.aborted = false;
    this.outputEl.innerHTML = '';
    this.currentStep = 0;

    for (let i = 0; i < demoSteps.length; i++) {
      if (this.aborted) break;
      this.currentStep = i;
      const step = demoSteps[i];

      if (step.type === 'pause') {
        await this.wait(step.duration);
      } else if (step.type === 'comment') {
        this.appendLine(step.text, 'demo-comment');
        await this.wait(400);
      } else if (step.type === 'command') {
        await this.typeCommand(step.text);
        await this.wait(300);
        if (step.result) {
          this.appendResult(step.result);
          await this.wait(200);
        }
      }

      this.scrollToBottom();
    }

    this.isRunning = false;
  }

  async typeCommand(text) {
    const lines = text.split('\n');
    for (let li = 0; li < lines.length; li++) {
      const line = lines[li];
      const lineEl = document.createElement('div');
      lineEl.className = 'demo-line';

      if (li === 0) {
        const prompt = document.createElement('span');
        prompt.className = 'demo-prompt';
        prompt.textContent = 'NovusDB> ';
        lineEl.appendChild(prompt);
      } else {
        const cont = document.createElement('span');
        cont.className = 'demo-continuation';
        cont.textContent = '     ... ';
        lineEl.appendChild(cont);
      }

      const cmdSpan = document.createElement('span');
      cmdSpan.className = 'demo-cmd';
      lineEl.appendChild(cmdSpan);
      this.outputEl.appendChild(lineEl);

      for (let c = 0; c < line.length; c++) {
        if (this.aborted) return;
        cmdSpan.textContent += line[c];
        this.scrollToBottom();
        await this.wait(this.typingSpeed);
      }
    }
  }

  appendLine(text, className) {
    const el = document.createElement('div');
    el.className = className || '';
    el.textContent = text;
    this.outputEl.appendChild(el);
  }

  appendResult(text) {
    const lines = text.split('\n');
    lines.forEach(line => {
      const el = document.createElement('div');
      el.className = 'demo-result';

      // Colorize output
      let html = line
        .replace(/(name|role|city|age|level|badge|product|total|revenue|orders|skills|meta\.level|date|label|user)=/g, '<span class="demo-field">$1</span>=')
        .replace(/="([^"]+)"/g, '=<span class="demo-string">"$1"</span>')
        .replace(/=(\d+)/g, '=<span class="demo-number">$1</span>')
        .replace(/=null/g, '=<span class="demo-null">null</span>')
        .replace(/(\[#\d+\])/g, '<span class="demo-id">$1</span>')
        .replace(/(---\s\d+\sdocument\(s\))/g, '<span class="demo-count">$1</span>')
        .replace(/(OK\s—.*)/g, '<span class="demo-ok">$1</span>')
        .replace(/(Index created.*)/g, '<span class="demo-ok">$1</span>')
        .replace(/(├─|└─|│)/g, '<span class="demo-tree">$1</span>')
        .replace(/(string|int64|array|object)/g, '<span class="demo-type">$1</span>')
        .replace(/(\d+\/\d+\s=\s\d+%)/g, '<span class="demo-pct">$1</span>')
        .replace(/(Hint applied:.*)/g, '<span class="demo-hint">$1</span>')
        .replace(/(strategy:\s\w+)/g, '<span class="demo-hint">$1</span>')
        .replace(/(Hit rate\s:\s[\d.]+%)/g, '<span class="demo-ok">$1</span>');

      el.innerHTML = html;
      this.outputEl.appendChild(el);
    });
  }

  scrollToBottom() {
    this.outputEl.scrollTop = this.outputEl.scrollHeight;
  }

  wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  stop() {
    this.aborted = true;
    this.isRunning = false;
  }
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  const container = document.getElementById('demo-terminal');
  if (!container) return;

  const demo = new TerminalDemo(container);

  const playBtn = container.querySelector('.demo-play');
  const stopBtn = container.querySelector('.demo-stop');

  // Auto-start after a short delay (hero is visible on load)
  setTimeout(() => demo.start(), 800);

  if (playBtn) {
    playBtn.addEventListener('click', () => {
      demo.stop();
      setTimeout(() => demo.start(), 100);
    });
  }
  if (stopBtn) {
    stopBtn.addEventListener('click', () => demo.stop());
  }
});
