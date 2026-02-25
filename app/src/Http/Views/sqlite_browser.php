<?php
declare(strict_types=1);
?>
<div class="card">
  <h2>SQLite Browser</h2>
  <div class="btn-row" style="align-items: center;">
    <label for="table-select">Tabelle auswählen</label>
    <select id="table-select"></select>
    <input id="search-input" type="text" placeholder="Suche" />
    <button class="btn" id="search-btn" type="button">Suchen</button>
    <span class="muted" id="loading-indicator" style="display:none;">Lade…</span>
  </div>
  <div class="btn-row" style="align-items: center; margin-top: 10px;">
    <span class="muted">Total: <strong id="total-count">0</strong></span>
    <span class="muted">Seite: <strong id="page-current">1</strong></span>
    <label for="per-page">pro Seite</label>
    <select id="per-page">
      <option value="25">25</option>
      <option value="50" selected>50</option>
      <option value="100">100</option>
      <option value="200">200</option>
    </select>
  </div>
  <div id="error-box" class="error" style="margin-top:10px; display:none;"></div>
</div>

<div class="card flex full-height">
  <div class="table-wrap table-scroll">
    <table id="data-table">
      <thead></thead>
      <tbody></tbody>
    </table>
  </div>
  <?php require __DIR__ . '/_paginator.php'; ?>
</div>

<script>
  (function () {
    const tableSelect = document.getElementById('table-select');
    const searchInput = document.getElementById('search-input');
    const searchBtn = document.getElementById('search-btn');
    const loadingIndicator = document.getElementById('loading-indicator');
    const errorBox = document.getElementById('error-box');
    const dataTable = document.getElementById('data-table');
    const thead = dataTable.querySelector('thead');
    const tbody = dataTable.querySelector('tbody');
    const prevBtn = document.getElementById('prev-btn');
    const nextBtn = document.getElementById('next-btn');
    const pageWindow = document.getElementById('page-window');
    const totalCount = document.getElementById('total-count');
    const pageCurrent = document.getElementById('page-current');
    const perPageSelect = document.getElementById('per-page');

    const state = {
      table: '',
      q: '',
      page: 1,
      perPage: 50,
      totalRows: 0
    };

    const setLoading = (isLoading) => {
      loadingIndicator.style.display = isLoading ? 'inline' : 'none';
      searchBtn.disabled = isLoading;
      tableSelect.disabled = isLoading;
      perPageSelect.disabled = isLoading;
      prevBtn.disabled = isLoading;
      nextBtn.disabled = isLoading;
    };

    const showError = (message) => {
      if (!message) {
        errorBox.style.display = 'none';
        errorBox.textContent = '';
        return;
      }
      errorBox.style.display = 'block';
      errorBox.textContent = message;
    };

    const renderTable = (columns, rows) => {
      thead.innerHTML = '';
      tbody.innerHTML = '';

      const headRow = document.createElement('tr');
      columns.forEach((col) => {
        const th = document.createElement('th');
        th.textContent = col;
        headRow.appendChild(th);
      });
      thead.appendChild(headRow);

      if (!rows.length) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = Math.max(columns.length, 1);
        td.textContent = 'Keine Einträge';
        tr.appendChild(td);
        tbody.appendChild(tr);
        return;
      }

      rows.forEach((row) => {
        const tr = document.createElement('tr');
        columns.forEach((col) => {
          const td = document.createElement('td');
          const value = row[col];
          td.textContent = value === null || value === undefined ? '' : String(value);
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    };

    const buildPageWindow = (current, total) => {
      const pages = [];
      const add = (value) => pages.push(value);
      if (total <= 7) {
        for (let i = 1; i <= total; i++) add(i);
        return pages;
      }
      add(1);
      if (current > 4) add('...');
      const start = Math.max(2, current - 1);
      const end = Math.min(total - 1, current + 1);
      for (let i = start; i <= end; i++) add(i);
      if (current < total - 3) add('...');
      add(total);
      return pages;
    };

    const renderPaginator = () => {
      const totalPages = Math.max(1, Math.ceil(state.totalRows / state.perPage));
      pageWindow.innerHTML = '';
      const pages = buildPageWindow(state.page, totalPages);
      pages.forEach((p) => {
        if (p === '...') {
          const span = document.createElement('span');
          span.className = 'muted';
          span.textContent = '...';
          pageWindow.appendChild(span);
          return;
        }
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'btn';
        if (p === state.page) {
          btn.disabled = true;
        }
        btn.textContent = String(p);
        btn.addEventListener('click', async () => {
          state.page = p;
          await loadTableData();
        });
        pageWindow.appendChild(btn);
      });

      prevBtn.disabled = state.page <= 1;
      nextBtn.disabled = state.page >= totalPages;
      totalCount.textContent = String(state.totalRows);
      pageCurrent.textContent = String(state.page);
    };

    const loadTableData = async () => {
      if (!state.table) return;
      setLoading(true);
      showError('');
      try {
        const params = new URLSearchParams({
          name: state.table,
          page: String(state.page),
          per_page: String(state.perPage)
        });
        if (state.q) {
          params.set('q', state.q);
        }
        const response = await fetch('/api/sqlite/table?' + params.toString(), {
          headers: { 'Accept': 'application/json' }
        });
        const json = await response.json();
        if (!response.ok) {
          throw new Error(json.error || 'Fehler beim Laden der Tabelle.');
        }

        state.totalRows = json.totalRows || 0;
        renderTable(json.columns || [], json.rows || []);
        renderPaginator();
      } catch (e) {
        showError(e.message);
      } finally {
        setLoading(false);
      }
    };

    const loadTables = async () => {
      setLoading(true);
      showError('');
      try {
        const response = await fetch('/api/sqlite/tables', { headers: { 'Accept': 'application/json' } });
        const json = await response.json();
        if (!response.ok) {
          throw new Error(json.error || 'Fehler beim Laden der Tabellen.');
        }

        tableSelect.innerHTML = '';
        (json || []).forEach((name) => {
          const option = document.createElement('option');
          option.value = name;
          option.textContent = name;
          tableSelect.appendChild(option);
        });

        if (json.length) {
          state.table = json[0];
          tableSelect.value = state.table;
          state.page = 1;
          state.q = '';
          searchInput.value = '';
          await loadTableData();
        }
      } catch (e) {
        showError(e.message);
      } finally {
        setLoading(false);
      }
    };

    tableSelect.addEventListener('change', async () => {
      state.table = tableSelect.value;
      state.page = 1;
      state.q = '';
      searchInput.value = '';
      await loadTableData();
    });

    searchBtn.addEventListener('click', async () => {
      state.q = searchInput.value.trim();
      state.page = 1;
      await loadTableData();
    });

    searchInput.addEventListener('keydown', async (event) => {
      if (event.key === 'Enter') {
        state.q = searchInput.value.trim();
        state.page = 1;
        await loadTableData();
      }
    });

    perPageSelect.addEventListener('change', async () => {
      state.perPage = parseInt(perPageSelect.value, 10) || 50;
      state.page = 1;
      await loadTableData();
    });

    prevBtn.addEventListener('click', async () => {
      if (state.page > 1) {
        state.page -= 1;
        await loadTableData();
      }
    });

    nextBtn.addEventListener('click', async () => {
      const totalPages = Math.max(1, Math.ceil(state.totalRows / state.perPage));
      if (state.page < totalPages) {
        state.page += 1;
        await loadTableData();
      }
    });

    loadTables();
  })();
</script>
