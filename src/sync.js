'use strict';

const { createClient } = require('@supabase/supabase-js');

// ─── Configuration ─────────────────────────────────────

const SOURCES = [
  {
    empresa_id: 'RT',
    url: process.env.RT_SUPABASE_URL,
    key: process.env.RT_SUPABASE_ANON_KEY,
  },
  {
    empresa_id: 'BLUELINE',
    url: process.env.BLUELINE_SUPABASE_URL,
    key: process.env.BLUELINE_SUPABASE_ANON_KEY,
  },
];

const DASH_URL = process.env.DASHBOARD_SUPABASE_URL;
const DASH_KEY = process.env.DASHBOARD_SUPABASE_SERVICE_KEY;

const BATCH_SIZE = 500;
const PAGE_SIZE = 1000;

// ─── Table mappings ────────────────────────────────────

const TABLES = [
  {
    source: 'omie_categories',
    target: 'dash_categories',
    conflict: 'empresa_id,codigo',
    columns: 'omie_id,codigo,descricao,descricao_padrao,natureza,tipo_categoria,codigo_dre,conta_despesa,conta_receita,totalizadora,transferencia,nao_exibir,inativa,definida_usuario,id_conta_contabil,codigo_centro_receita_despesa',
  },
  {
    source: 'omie_financial_movements',
    target: 'dash_financial_movements',
    conflict: 'empresa_id,cod_titulo,numero_parcela',
    columns: 'omie_id,cod_titulo,cod_int_titulo,num_titulo,natureza,tipo,status,origem,operacao,cod_cliente,cpf_cnpj_cliente,cod_categoria,cod_cc,dt_emissao,dt_vencimento,dt_previsao,dt_pagamento,dt_registro,dt_credito,dt_conciliacao,cod_origem,cod_projeto,cod_pedido,cod_os,cod_titulo_repet,valor_titulo,percent_juros,percent_multa,percent_desconto,valor_pis,valor_cofins,valor_csll,valor_ir,valor_iss,valor_inss,num_doc_fiscal,numero_parcela,observacao,cod_vendedor,liquidado,val_pago,val_aberto,val_liquido,val_juros,val_multa,val_desconto,codigo_barras,numero_boleto,nosso_numero,url_boleto,cod_departamento,percentual_departamento',
  },
  {
    source: 'omie_accounts_receivable',
    target: 'dash_accounts_receivable',
    conflict: 'empresa_id,codigo_lancamento_omie',
    columns: 'codigo_lancamento_omie,codigo_lancamento_integ,codigo_cliente_fornecedor,codigo_cliente_integ,numero_documento,numero_documento_fiscal,numero_parcela,data_emissao,data_vencimento,data_previsao,data_pagamento,data_registro,valor_documento,valor_recebido,valor_pis,valor_cofins,valor_csll,valor_ir,valor_iss,valor_inss,percentual_juros,data_juros,codigo_categoria,codigo_tipo_documento,codigo_conta_corrente,codigo_projeto,codigo_vendedor,codigo_departamento,status_titulo,operacao,observacao,codigo_barras,numero_boleto,codigo_barras_dac,nosso_numero',
  },
  {
    source: 'omie_accounts_payable',
    target: 'dash_accounts_payable',
    conflict: 'empresa_id,codigo_lancamento_omie',
    columns: 'codigo_lancamento_omie,codigo_lancamento_integ,codigo_cliente_fornecedor,codigo_cliente_integ,numero_documento,numero_documento_fiscal,numero_parcela,data_emissao,data_vencimento,data_previsao,data_pagamento,data_registro,valor_documento,valor_pago,valor_pis,valor_cofins,valor_csll,valor_ir,valor_iss,valor_inss,codigo_categoria,codigo_tipo_documento,codigo_conta_corrente,codigo_projeto,codigo_vendedor,codigo_departamento,status_titulo,operacao,observacao,codigo_barras,numero_boleto,nosso_numero',
  },
  {
    source: 'omie_clients',
    target: 'dash_clients',
    conflict: 'empresa_id,codigo_cliente_omie',
    columns: 'codigo_cliente_omie,codigo_cliente_integracao,razao_social,nome_fantasia,cnpj_cpf,email,estado,cidade,bairro,endereco,cep,telefone1_ddd,telefone1_numero,pessoa_fisica,inativo,ativo',
  },
];

// ─── Helpers ───────────────────────────────────────────

async function fetchAll(client, table, columns) {
  let allData = [];
  let from = 0;

  while (true) {
    const { data, error } = await client
      .from(table)
      .select(columns)
      .range(from, from + PAGE_SIZE - 1);

    if (error) throw new Error(`Fetch ${table}: ${error.message}`);
    if (!data || data.length === 0) break;

    allData = allData.concat(data);
    if (data.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }

  return allData;
}

async function upsertBatch(client, table, rows, conflict) {
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const { error } = await client
      .from(table)
      .upsert(batch, { onConflict: conflict });

    if (error) {
      throw new Error(`Upsert ${table} (batch ${Math.floor(i / BATCH_SIZE) + 1}): ${error.message}`);
    }
  }
}

function log(msg) {
  const ts = new Date().toISOString().replace('T', ' ').slice(0, 19);
  console.log(`[${ts}] ${msg}`);
}

// ─── Main sync ─────────────────────────────────────────

async function syncSource(source, dashClient) {
  const { empresa_id, url, key } = source;

  log(`=== Starting sync: ${empresa_id} ===`);

  const srcClient = createClient(url, key);
  let totalRows = 0;

  for (const mapping of TABLES) {
    const { source: srcTable, target, conflict, columns } = mapping;

    try {
      // 1. Fetch from source
      log(`  [${empresa_id}] Reading ${srcTable}...`);
      const rows = await fetchAll(srcClient, srcTable, columns);
      log(`  [${empresa_id}] ${srcTable}: ${rows.length} rows`);

      if (rows.length === 0) {
        log(`  [${empresa_id}] ${srcTable}: skipped (empty)`);
        continue;
      }

      // 2. Add empresa_id and handle nulls in PK columns
      const enrichedRows = rows.map(row => {
        const enriched = { empresa_id, ...row };

        // Ensure numero_parcela is never null (PK constraint on dash_financial_movements)
        if (target === 'dash_financial_movements' && !enriched.numero_parcela) {
          enriched.numero_parcela = '001';
        }

        return enriched;
      });

      // 3. Upsert into dashboard
      log(`  [${empresa_id}] Writing ${target}...`);
      await upsertBatch(dashClient, target, enrichedRows, conflict);

      totalRows += rows.length;
      log(`  [${empresa_id}] ${target}: done (${rows.length} rows)`);

    } catch (err) {
      log(`  [${empresa_id}] ERROR ${srcTable}: ${err.message}`);
    }
  }

  log(`=== Finished ${empresa_id}: ${totalRows} total rows ===`);
  return totalRows;
}

async function main() {
  log('========================================');
  log('Dashboard Sync - Starting');
  log('========================================');

  // Validate config
  if (!DASH_URL || !DASH_KEY) {
    throw new Error('Missing DASHBOARD_SUPABASE_URL or DASHBOARD_SUPABASE_SERVICE_KEY');
  }

  const dashClient = createClient(DASH_URL, DASH_KEY);

  let grandTotal = 0;
  const results = [];

  for (const source of SOURCES) {
    if (!source.url || !source.key) {
      log(`Skipping ${source.empresa_id}: missing URL or key`);
      continue;
    }

    try {
      const count = await syncSource(source, dashClient);
      results.push({ empresa: source.empresa_id, rows: count, status: 'success' });
      grandTotal += count;
    } catch (err) {
      log(`ERROR ${source.empresa_id}: ${err.message}`);
      results.push({ empresa: source.empresa_id, rows: 0, status: 'error', error: err.message });
    }
  }

  log('========================================');
  log(`Dashboard Sync - Complete: ${grandTotal} rows`);
  log('========================================');
  console.log(JSON.stringify(results, null, 2));
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
