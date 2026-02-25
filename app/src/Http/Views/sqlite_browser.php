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
  <div id="error-box" class="error" style="margin-top:10px; display:none;"></div>
</div>

<div class="card">
  <div class="table-wrap">
    <table id="data-table">
      <thead></thead>
      <tbody></tbody>
    </table>
  </div>
  <div class="pagination">
    <button class="btn" id="prev-btn" type="button">Zurück</button>
    <button class="btn" id="next-btn" type="button">Weiter</button>
    <span class="muted" id="page-info">Seite 1</span>
  </div>
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
    const pageInfo = document.getElementById('page-info');

    const state = {
      table: '',
      q: '',
      page: 1,
      pageSize: 50,
      totalRows: 0
    };

    const setLoading = (isLoading) => {
      loadingIndicator.style.display = isLoading ? 'inline' : 'none';
      searchBtn.disabled = isLoading;
      tableSelect.disabled = isLoading;
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

    const updatePagination = () => {
      const totalPages = Math.max(1, Math.ceil(state.totalRows / state.pageSize));
      pageInfo.textContent = 'Seite ' + state.page + ' von ' + totalPages;
      prevBtn.disabled = state.page <= 1;
      nextBtn.disabled = state.page >= totalPages;
    };

    const loadTableData = async () => {
      if (!state.table) return;
      setLoading(true);
      showError('');
      try {
        const params = new URLSearchParams({
          name: state.table,
          page: String(state.page),
          pageSize: String(state.pageSize)
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
        updatePagination();
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

    prevBtn.addEventListener('click', async () => {
      if (state.page > 1) {
        state.page -= 1;
        await loadTableData();
      }
    });

    nextBtn.addEventListener('click', async () => {
      const totalPages = Math.max(1, Math.ceil(state.totalRows / state.pageSize));
      if (state.page < totalPages) {
        state.page += 1;
        await loadTableData();
      }
    });

    loadTables();
  })();
</script>
