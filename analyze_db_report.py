"""
Анализ статистики PostgreSQL из Excel отчета
Анализирует данные как опытный DBA и генерирует отчет
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import argparse
import glob


# Глобальные настройки
DEFAULT_EXCEL_FILE = "20 RPS.xlsx"  # Файл по умолчанию


class PostgresAnalyzer:
    """Анализатор статистики PostgreSQL"""
    
    def __init__(self, excel_file):
        self.excel_file = excel_file
        self.sheets = {}
        self.load_data()
    
    def load_data(self):
        """Загружает все листы из Excel файла"""
        print("Загрузка данных из Excel...")
        xl_file = pd.ExcelFile(self.excel_file)
        
        for sheet_name in xl_file.sheet_names:
            try:
                self.sheets[sheet_name] = pd.read_excel(xl_file, sheet_name=sheet_name)
                print(f"  ✓ {sheet_name}: {len(self.sheets[sheet_name])} строк")
            except Exception as e:
                print(f"  ✗ Ошибка загрузки {sheet_name}: {e}")
    
    def get_report_period(self):
        """Получает период отчета"""
        props = self.sheets.get('Properties', pd.DataFrame())
        if props.empty:
            return "Не указан", "Не указан", 0
        
        start = props['report_start1'].iloc[0] if 'report_start1' in props.columns else "Не указан"
        end = props['report_end1'].iloc[0] if 'report_end1' in props.columns else "Не указан"
        
        # Вычисляем длительность в минутах
        try:
            duration_sec = props['interval_duration_sec'].iloc[0]
            duration_min = int(duration_sec / 60)
        except:
            duration_min = 0
        
        return start, end, duration_min
    
    def analyze_database_stats(self):
        """Анализирует общую статистику БД"""
        df = self.sheets.get('dbstat', pd.DataFrame())
        if df.empty:
            return []
        
        results = []
        
        for _, row in df.iterrows():
            dbname = row.get('dbname', 'Unknown')
            
            # Основные метрики
            cache_hit_ratio = row.get('blks_hit_pct', 0)
            size = row.get('datsize', 'N/A')
            size_delta = row.get('datsize_delta', 'N/A')
            commits = row.get('xact_commit', 0)
            rollbacks = row.get('xact_rollback', 0)
            deadlocks = row.get('deadlocks', 0)
            temp_files = row.get('temp_files', 0)
            temp_bytes = row.get('temp_bytes', 0)
            
            # Оценка проблем
            issues = []
            if cache_hit_ratio < 95:
                issues.append(f"⚠️ Низкий cache hit ratio: {cache_hit_ratio:.2f}%")
            if deadlocks and deadlocks > 0:
                issues.append(f"⚠️ Обнаружены deadlocks: {deadlocks}")
            if temp_files and temp_files > 0:
                issues.append(f"⚠️ Использование временных файлов: {temp_files} ({temp_bytes})")
            
            rollback_ratio = (rollbacks / (commits + rollbacks) * 100) if (commits + rollbacks) > 0 else 0
            if rollback_ratio > 5:
                issues.append(f"⚠️ Высокий процент rollback: {rollback_ratio:.2f}%")
            
            results.append({
                'dbname': dbname,
                'size': size,
                'size_delta': size_delta,
                'cache_hit_ratio': cache_hit_ratio,
                'commits': commits,
                'rollbacks': rollbacks,
                'rollback_ratio': rollback_ratio,
                'deadlocks': deadlocks,
                'temp_files': temp_files,
                'temp_bytes': temp_bytes,
                'issues': issues
            })
        
        return results
    
    def get_query_text(self, query_id):
        """Получает текст запроса по его ID"""
        queries_df = self.sheets.get('queries', pd.DataFrame())
        if queries_df.empty:
            return None
        
        query_row = queries_df[queries_df['hexqueryid'] == query_id]
        if not query_row.empty:
            query_texts = query_row['query_texts'].iloc[0]
            if isinstance(query_texts, str) and query_texts:
                # Очищаем от лишних пробелов и переносов
                text = ' '.join(query_texts.split())
                return text
        return None
    
    def analyze_top_queries(self, top_n=10):
        """Анализирует самые тяжелые запросы"""
        df = self.sheets.get('top_statements', pd.DataFrame())
        if df.empty:
            return []
        
        # Проверяем наличие столбцов
        time_col = 'total_exec_time' if 'total_exec_time' in df.columns else 'total_time'
        mean_col = 'mean_exec_time' if 'mean_exec_time' in df.columns else 'mean_time'
        
        # Сортируем по общему времени выполнения
        df_sorted = df.sort_values(by=time_col, ascending=False).head(top_n)
        
        results = []
        for _, row in df_sorted.iterrows():
            query_id = row.get('hexqueryid', 'N/A')
            dbname = row.get('dbname', 'N/A')
            username = row.get('username', 'N/A')
            calls = row.get('calls', 0)
            total_time = row.get(time_col, 0)
            mean_time = row.get(mean_col, 0)
            rows = row.get('rows', 0)
            
            # Получаем текст запроса
            query_text = self.get_query_text(query_id)
            query_preview = query_text[:50] if query_text else 'N/A'
            query_preview_suffix = '...' if query_text and len(query_text) > 50 else ''
            
            # Анализ I/O
            shared_blks_hit = row.get('shared_blks_hit', 0)
            shared_blks_read = row.get('shared_blks_read', 0)
            temp_blks_written = row.get('temp_blks_written', 0)
            
            # Расчет cache hit ratio для запроса
            total_blks = shared_blks_hit + shared_blks_read
            query_cache_ratio = (shared_blks_hit / total_blks * 100) if total_blks > 0 else 100
            
            # Проблемы
            issues = []
            if mean_time > 1000:
                issues.append(f"Медленный запрос: {mean_time:.2f} мс")
            if temp_blks_written > 0:
                issues.append(f"Использует temp: {temp_blks_written} блоков")
            if query_cache_ratio < 90:
                issues.append(f"Низкий cache hit: {query_cache_ratio:.1f}%")
            
            results.append({
                'query_id': query_id,
                'query_preview': query_preview,
                'query_preview_suffix': query_preview_suffix,
                'dbname': dbname,
                'username': username,
                'calls': calls,
                'total_time': total_time,
                'mean_time': mean_time,
                'rows': rows,
                'cache_ratio': query_cache_ratio,
                'temp_blks': temp_blks_written,
                'issues': issues
            })
        
        return results
    
    def analyze_top_wal_queries(self, top_n=5):
        """Анализирует топ запросов по генерации WAL"""
        df = self.sheets.get('top_statements', pd.DataFrame())
        if df.empty:
            return []
        
        # Проверяем наличие колонки wal_bytes
        if 'wal_bytes' not in df.columns:
            return []
        
        # Фильтруем только запросы с WAL активностью
        df_wal = df[df['wal_bytes'].notna() & (df['wal_bytes'] > 0)].copy()
        
        if df_wal.empty:
            return []
        
        # Сортируем по wal_bytes
        df_sorted = df_wal.sort_values(by='wal_bytes', ascending=False).head(top_n)
        
        results = []
        for _, row in df_sorted.iterrows():
            query_id = row.get('hexqueryid', 'N/A')
            wal_bytes = row.get('wal_bytes', 0)
            wal_bytes_pct = row.get('wal_bytes_pct', 0)
            
            query_text = self.get_query_text(query_id)
            query_preview = query_text[:50] if query_text else 'N/A'
            query_preview_suffix = '...' if query_text and len(query_text) > 50 else ''
            
            # Конвертируем в MB/GB
            wal_mb = wal_bytes / (1024 * 1024)
            wal_gb = wal_mb / 1024 if wal_mb > 1024 else 0
            
            results.append({
                'query_id': query_id,
                'query_preview': query_preview,
                'query_preview_suffix': query_preview_suffix,
                'dbname': row.get('dbname', 'N/A'),
                'calls': row.get('calls', 0),
                'wal_bytes': wal_bytes,
                'wal_mb': round(wal_mb, 2),
                'wal_gb': round(wal_gb, 3) if wal_gb > 0 else 0,
                'wal_pct': round(wal_bytes_pct, 2) if wal_bytes_pct else 0
            })
        
        return results
    
    def analyze_wal_stats(self):
        """Анализирует статистику WAL"""
        df = self.sheets.get('wal_stats', pd.DataFrame())
        if df.empty:
            return {}
        
        row = df.iloc[0]
        
        wal_records = row.get('wal_records', 0)
        wal_fpi = row.get('wal_fpi', 0)
        wal_bytes = row.get('wal_bytes', 0)
        wal_write_time = row.get('wal_write_time', 0)
        wal_sync_time = row.get('wal_sync_time', 0)
        
        # Конвертация в MB/GB
        wal_mb = wal_bytes / (1024 * 1024) if wal_bytes else 0
        wal_gb = wal_mb / 1024
        
        return {
            'records': wal_records,
            'fpi': wal_fpi,
            'bytes': wal_bytes,
            'size_mb': wal_mb,
            'size_gb': wal_gb,
            'write_time': wal_write_time,
            'sync_time': wal_sync_time
        }
    
    def analyze_tables(self, top_n=10):
        """Анализирует статистику таблиц"""
        df = self.sheets.get('top_tables', pd.DataFrame())
        if df.empty:
            return []
        
        results = []
        
        for _, row in df.iterrows():
            dbname = row.get('dbname', 'N/A')
            schemaname = row.get('schemaname', 'N/A')
            relname = row.get('relname', 'N/A')
            
            n_live_tup = row.get('n_live_tup', 0)
            n_dead_tup = row.get('n_dead_tup', 0)
            n_mod_since_analyze = row.get('n_mod_since_analyze', 0)
            
            seq_scan = row.get('seq_scan', 0)
            idx_scan = row.get('idx_scan', 0)
            
            relsize = row.get('relsize', 'N/A')
            
            # Проблемы
            issues = []
            
            # Bloat проблема
            if n_live_tup > 0:
                dead_ratio = (n_dead_tup / n_live_tup * 100)
                if dead_ratio > 20:
                    issues.append(f"⚠️ Много мертвых строк: {dead_ratio:.1f}% ({n_dead_tup:,})")
            
            # Проблема с ANALYZE
            if n_live_tup > 0 and n_mod_since_analyze > n_live_tup * 0.2:
                issues.append(f"⚠️ Нужен ANALYZE: {n_mod_since_analyze:,} изменений")
            
            # Seq scan на больших таблицах
            if seq_scan > 100 and n_live_tup > 10000:
                issues.append(f"⚠️ Много seq_scan: {seq_scan} (возможно нужен индекс)")
            
            if issues:  # Добавляем только проблемные таблицы
                results.append({
                    'dbname': dbname,
                    'schema': schemaname,
                    'table': relname,
                    'size': relsize,
                    'live_tuples': n_live_tup,
                    'dead_tuples': n_dead_tup,
                    'seq_scan': seq_scan,
                    'idx_scan': idx_scan,
                    'mod_since_analyze': n_mod_since_analyze,
                    'issues': issues
                })
        
        return sorted(results, key=lambda x: len(x['issues']), reverse=True)[:top_n]
    
    def analyze_indexes(self):
        """Анализирует использование индексов"""
        df = self.sheets.get('top_indexes', pd.DataFrame())
        if df.empty:
            return []
        
        results = []
        
        for _, row in df.iterrows():
            idx_scan = row.get('idx_scan', 0)
            
            # Ищем неиспользуемые индексы
            if idx_scan == 0:
                results.append({
                    'dbname': row.get('dbname', 'N/A'),
                    'schema': row.get('schemaname', 'N/A'),
                    'table': row.get('relname', 'N/A'),
                    'index': row.get('indexrelname', 'N/A'),
                    'size': row.get('indexrelsize', 'N/A'),
                    'scans': idx_scan
                })
        
        return results
    
    def generate_markdown_report(self, output_file):
        """Генерирует Markdown отчет"""
        print(f"\nГенерация отчета в {output_file}...")
        
        start, end, duration = self.get_report_period()
        db_stats = self.analyze_database_stats()
        top_queries = self.analyze_top_queries(10)
        wal_stats = self.analyze_wal_stats()
        top_wal_queries = self.analyze_top_wal_queries(5)
        problem_tables = self.analyze_tables(10)
        unused_indexes = self.analyze_indexes()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            # Заголовок
            f.write("# 📊 Анализ производительности PostgreSQL\n\n")
            f.write(f"**Дата анализа**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Период отчета
            f.write("## ⏱️ Период мониторинга\n\n")
            f.write(f"- **Начало**: `{start}`\n")
            f.write(f"- **Конец**: `{end}`\n")
            f.write(f"- **Длительность**: {duration} минут\n\n")
            
            f.write("---\n\n")
            
            # Общая статистика БД
            f.write("## 🗄️ Общая статистика баз данных\n\n")
            
            for db in db_stats:
                f.write(f"### База данных: `{db['dbname']}`\n\n")
                f.write(f"| Метрика | Значение |\n")
                f.write(f"|---------|----------|\n")
                
                # Форматируем все значения, заменяя nan на пустую строку
                size_str = '' if pd.isna(db['size']) else str(db['size'])
                size_delta_str = '' if pd.isna(db['size_delta']) else str(db['size_delta'])
                commits_str = f"{int(db['commits']):,}" if pd.notna(db['commits']) else ''
                rollbacks_str = f"{int(db['rollbacks']):,}" if pd.notna(db['rollbacks']) else ''
                rollback_ratio_str = f"({db['rollback_ratio']:.2f}%)" if pd.notna(db['rollback_ratio']) and rollbacks_str else ''
                deadlocks_str = '' if pd.isna(db['deadlocks']) or db['deadlocks'] == 0 else str(int(db['deadlocks']))
                temp_files_str = '' if pd.isna(db['temp_files']) or db['temp_files'] == 0 else str(int(db['temp_files']))
                
                f.write(f"| **Размер БД** | {size_str} |\n")
                f.write(f"| **Изменение размера** | {size_delta_str} |\n")
                f.write(f"| **Cache Hit Ratio** | {db['cache_hit_ratio']:.2f}% |\n")
                f.write(f"| **Commits** | {commits_str} |\n")
                f.write(f"| **Rollbacks** | {rollbacks_str} {rollback_ratio_str} |\n")
                f.write(f"| **Deadlocks** | {deadlocks_str} |\n")
                f.write(f"| **Временные файлы** | {temp_files_str} |\n\n")
                
                if db['issues']:
                    f.write("**⚠️ Обнаруженные проблемы:**\n\n")
                    for issue in db['issues']:
                        f.write(f"- {issue}\n")
                    f.write("\n")
                else:
                    f.write("✅ **Проблем не обнаружено**\n\n")
            
            f.write("---\n\n")
            
            # WAL статистика
            f.write("## 📝 Статистика Write-Ahead Log (WAL)\n\n")
            
            if wal_stats:
                f.write(f"| Метрика | Значение |\n")
                f.write(f"|---------|----------|\n")
                f.write(f"| **Количество записей** | {wal_stats['records']:,} |\n")
                f.write(f"| **Full Page Images** | {wal_stats['fpi']:,} |\n")
                f.write(f"| **Объем WAL** | {wal_stats['size_mb']:.2f} MB ({wal_stats['size_gb']:.3f} GB) |\n")
                f.write(f"| **Время записи** | {wal_stats['write_time']:.2f} мс |\n")
                f.write(f"| **Время синхронизации** | {wal_stats['sync_time']:.2f} мс |\n\n")
                
                # Анализ
                wal_per_min = wal_stats['size_mb'] / duration if duration > 0 else 0
                f.write(f"**Скорость генерации WAL**: {wal_per_min:.2f} MB/мин\n\n")
                
                if wal_per_min > 100:
                    f.write("⚠️ **Высокая скорость генерации WAL** - возможно много операций записи\n\n")
                elif wal_per_min > 50:
                    f.write("⚡ **Умеренная активность записи**\n\n")
                else:
                    f.write("✅ **Нормальная активность записи**\n\n")
            else:
                f.write("*Данные недоступны*\n\n")
            
            # Топ запросов по генерации WAL
            if top_wal_queries:
                f.write("### 📊 Топ-5 запросов по генерации WAL\n\n")
                f.write("*Запросы с наибольшим объемом Write-Ahead Log*\n\n")
                
                for i, query in enumerate(top_wal_queries, 1):
                    f.write(f"**{i}. Query ID:** `{query['query_id']}`\n\n")
                    f.write(f"- **SQL Preview:** `{query['query_preview']}{query['query_preview_suffix']}`\n")
                    f.write(f"- **База данных:** {query['dbname']}\n")
                    f.write(f"- **Количество вызовов:** {query['calls']:,}\n")
                    f.write(f"- **Объем WAL:** {query['wal_mb']:.2f} MB")
                    
                    if query['wal_pct'] > 0:
                        f.write(f" — {query['wal_pct']:.1f}% от общего WAL")
                    
                    f.write("\n\n")
                
                f.write("\n")
            
            f.write("---\n\n")
            
            # Топ тяжелых запросов
            f.write("## 🔥 Топ самых тяжелых запросов\n\n")
            
            if top_queries:
                f.write(f"*Анализ {len(top_queries)} запросов с наибольшим временем выполнения*\n\n")
                
                for i, query in enumerate(top_queries, 1):
                    f.write(f"### {i}. Query ID: `{query['query_id']}`\n\n")
                    f.write(f"**SQL Preview:** `{query['query_preview']}{query['query_preview_suffix']}`\n\n")
                    f.write(f"| Параметр | Значение |\n")
                    f.write(f"|----------|----------|\n")
                    f.write(f"| **База данных** | {query['dbname']} |\n")
                    f.write(f"| **Пользователь** | {query['username']} |\n")
                    f.write(f"| **Количество вызовов** | {query['calls']:,} |\n")
                    f.write(f"| **Общее время выполнения** | {query['total_time']*1000:.0f} мс |\n")
                    f.write(f"| **Среднее время** | {query['mean_time']:.2f} мс |\n")
                    
                    # Форматируем количество строк с проверкой на NaN
                    rows_value = query['rows']
                    rows_str = f"{int(rows_value):,}" if pd.notna(rows_value) and rows_value > 0 else ""
                    f.write(f"| **Количество строк** | {rows_str} |\n")
                    
                    f.write(f"| **Cache Hit Ratio** | {query['cache_ratio']:.1f}% |\n")
                    
                    if query['temp_blks'] > 0:
                        f.write(f"| **Временные блоки** | {query['temp_blks']:,} |\n")
                    
                    f.write("\n")
                    
                    if query['issues']:
                        f.write("**⚠️ Проблемы:**\n\n")
                        for issue in query['issues']:
                            f.write(f"- {issue}\n")
                        f.write("\n")
                    else:
                        f.write("✅ **Запрос работает нормально**\n\n")
                    
                    # Рекомендации
                    recommendations = []
                    if query['mean_time'] > 1000:
                        recommendations.append("Рассмотреть оптимизацию запроса или добавление индексов")
                    if query['temp_blks'] > 0:
                        recommendations.append("Увеличить `work_mem` для избежания использования временных файлов")
                    if query['cache_ratio'] < 90:
                        recommendations.append("Проверить индексы и статистику таблиц")
                    
                    if recommendations:
                        f.write("**💡 Рекомендации:**\n\n")
                        for rec in recommendations:
                            f.write(f"- {rec}\n")
                        f.write("\n")
                    
                    f.write("---\n\n")
            else:
                f.write("*Данные о запросах недоступны*\n\n")
            
            # Проблемные таблицы
            if problem_tables:
                f.write("## 🗂️ Таблицы требующие внимания\n\n")
                
                for i, table in enumerate(problem_tables, 1):
                    f.write(f"### {i}. `{table['schema']}.{table['table']}`\n\n")
                    f.write(f"| Параметр | Значение |\n")
                    f.write(f"|----------|----------|\n")
                    f.write(f"| **База данных** | {table['dbname']} |\n")
                    f.write(f"| **Размер** | {table['size']} |\n")
                    f.write(f"| **Живых строк** | {table['live_tuples']:,} |\n")
                    f.write(f"| **Мертвых строк** | {table['dead_tuples']:,} |\n")
                    f.write(f"| **Seq Scan** | {table['seq_scan']:,} |\n")
                    f.write(f"| **Index Scan** | {table['idx_scan']:,} |\n")
                    f.write(f"| **Изменений с ANALYZE** | {table['mod_since_analyze']:,} |\n\n")
                    
                    f.write("**⚠️ Проблемы:**\n\n")
                    for issue in table['issues']:
                        f.write(f"- {issue}\n")
                    f.write("\n")
                    
                    # Рекомендации
                    f.write("**💡 Рекомендации:**\n\n")
                    if table['dead_tuples'] > table['live_tuples'] * 0.2:
                        f.write(f"- Выполнить `VACUUM ANALYZE {table['schema']}.{table['table']};`\n")
                    if table['mod_since_analyze'] > table['live_tuples'] * 0.2:
                        f.write(f"- Выполнить `ANALYZE {table['schema']}.{table['table']};`\n")
                    if table['seq_scan'] > 100 and table['live_tuples'] > 10000:
                        f.write(f"- Рассмотреть создание индекса для частых запросов\n")
                    f.write("\n")
                    
                    f.write("---\n\n")
            
            # Неиспользуемые индексы
            if unused_indexes:
                f.write("## 🔍 Неиспользуемые индексы\n\n")
                f.write("*Индексы, которые не использовались за период мониторинга*\n\n")
                
                f.write("| База данных | Схема | Таблица | Индекс | Размер |\n")
                f.write("|-------------|-------|---------|--------|--------|\n")
                
                for idx in unused_indexes[:10]:
                    f.write(f"| {idx['dbname']} | {idx['schema']} | {idx['table']} | {idx['index']} | {idx['size']} |\n")
                
                f.write("\n**💡 Рекомендация**: Рассмотреть удаление неиспользуемых индексов для экономии места и улучшения производительности INSERT/UPDATE операций.\n\n")
                f.write("```sql\n")
                f.write("-- Проверьте использование индекса перед удалением:\n")
                for idx in unused_indexes[:3]:
                    f.write(f"DROP INDEX IF EXISTS {idx['schema']}.{idx['index']};\n")
                f.write("```\n\n")
                f.write("---\n\n")
            
            # Общие выводы и рекомендации
            f.write("## 📋 Общие выводы и рекомендации\n\n")
            
            f.write("### ✅ Что работает хорошо\n\n")
            
            good_things = []
            for db in db_stats:
                if db['cache_hit_ratio'] >= 95:
                    good_things.append(f"Отличный cache hit ratio в БД `{db['dbname']}`: {db['cache_hit_ratio']:.2f}%")
                if db['deadlocks'] == 0:
                    good_things.append(f"Нет deadlocks в БД `{db['dbname']}`")
            
            if not good_things:
                good_things.append("База работает в целом стабильно")
            
            for item in good_things:
                f.write(f"- {item}\n")
            
            f.write("\n### ⚠️ Критические проблемы\n\n")
            
            critical = []
            for db in db_stats:
                if db['cache_hit_ratio'] < 90:
                    critical.append(f"**Очень низкий cache hit ratio** в `{db['dbname']}`: {db['cache_hit_ratio']:.2f}% - нужно увеличить `shared_buffers`")
                if db['deadlocks'] and db['deadlocks'] > 0:
                    critical.append(f"**Deadlocks** в `{db['dbname']}`: {db['deadlocks']} - проверить логику приложения")
            
            if not critical:
                f.write("- Критических проблем не обнаружено ✅\n")
            else:
                for item in critical:
                    f.write(f"- {item}\n")
            
            f.write("\n### 💡 Рекомендации по оптимизации\n\n")
            
            recommendations = []
            
            # Анализ для рекомендаций
            for db in db_stats:
                if db['temp_files'] and db['temp_files'] > 0:
                    recommendations.append("**Увеличить work_mem** - обнаружено использование временных файлов")
                if 90 <= db['cache_hit_ratio'] < 95:
                    recommendations.append(f"**Рассмотреть увеличение shared_buffers** - cache hit ratio {db['cache_hit_ratio']:.2f}% можно улучшить")
            
            if problem_tables:
                recommendations.append("**Настроить autovacuum** - обнаружены таблицы с большим количеством мертвых строк")
            
            if unused_indexes:
                recommendations.append(f"**Удалить {len(unused_indexes)} неиспользуемых индексов** - освободит место и ускорит операции записи")
            
            heavy_queries = [q for q in top_queries if q['mean_time'] > 1000]
            if heavy_queries:
                recommendations.append(f"**Оптимизировать {len(heavy_queries)} медленных запросов** - используйте EXPLAIN ANALYZE для анализа")
            
            if not recommendations:
                recommendations.append("База данных настроена хорошо, критических рекомендаций нет")
            
            for i, rec in enumerate(recommendations, 1):
                f.write(f"{i}. {rec}\n")
            
            f.write("\n---\n\n")
            
            # Футер
            f.write("## 📚 Дополнительная информация\n\n")
            f.write("**Источник данных**: `report--postgres-8360-8361.xlsx`\n\n")
            f.write("**Инструменты анализа**: Python, pandas, openpyxl\n\n")
            f.write("**Методология**: Анализ включает оценку производительности запросов, ")
            f.write("использования индексов, статистики таблиц, WAL активности и общего здоровья БД.\n\n")
            f.write(f"*Отчет сгенерирован автоматически {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")
        
        print(f"✓ Отчет успешно сохранен в {output_file}")


def process_excel_file(excel_file_path):
    """Обрабатывает один Excel файл и создает отчет"""
    excel_file = Path(excel_file_path)
    
    if not excel_file.exists():
        print(f"❌ Ошибка: файл {excel_file} не найден!")
        return False
    
    # Формируем имя выходного MD файла: ReportDB_(имя excel).md
    base_name = excel_file.stem  # Имя без расширения
    output_file = excel_file.parent / f"ReportDB_{base_name}.md"
    
    print("=" * 70)
    print(f"Анализ файла: {excel_file.name}")
    print("=" * 70)
    print()
    
    try:
        analyzer = PostgresAnalyzer(excel_file)
        analyzer.generate_markdown_report(output_file)
        
        print()
        print("=" * 70)
        print(f"✓ Файл {excel_file.name} успешно проанализирован!")
        print(f"  Создан отчет: {output_file.name}")
        print("=" * 70)
        print()
        
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка при анализе {excel_file.name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Основная функция"""
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(
        description='Анализ PostgreSQL статистики из Excel и генерация DBA отчета',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s                           # Использовать файл по умолчанию
  %(prog)s report.xlsx               # Проанализировать один файл
  %(prog)s *.xlsx                    # Проанализировать все Excel файлы в папке
  %(prog)s "20 RPS.xlsx" "40 RPS.xlsx"  # Проанализировать несколько файлов
  %(prog)s C:/reports/*.xlsx         # Проанализировать файлы по пути с маской
        """
    )
    
    parser.add_argument(
        'files',
        nargs='*',
        help=f'Путь к Excel файлу(ам) или маска (*.xlsx). По умолчанию: {DEFAULT_EXCEL_FILE}'
    )
    
    args = parser.parse_args()
    
    # Определяем список файлов для обработки
    files_to_process = []
    
    if args.files:
        # Обрабатываем каждый аргумент
        for file_pattern in args.files:
            # Проверяем, содержит ли паттерн wildcards
            if '*' in file_pattern or '?' in file_pattern:
                # Используем glob для поиска файлов
                matched_files = glob.glob(file_pattern)
                if matched_files:
                    files_to_process.extend(matched_files)
                else:
                    print(f"⚠️ Предупреждение: паттерн '{file_pattern}' не совпал ни с одним файлом")
            else:
                # Обычный файл
                files_to_process.append(file_pattern)
    else:
        # Используем файл по умолчанию
        default_path = Path(__file__).parent / DEFAULT_EXCEL_FILE
        files_to_process.append(str(default_path))
    
    if not files_to_process:
        print("❌ Ошибка: не указаны файлы для обработки!")
        parser.print_help()
        return
    
    print()
    print("=" * 70)
    print("Анализ PostgreSQL статистики из Excel")
    print("=" * 70)
    print(f"\nНайдено файлов для обработки: {len(files_to_process)}\n")
    
    # Обрабатываем каждый файл
    success_count = 0
    failed_count = 0
    
    for excel_file in files_to_process:
        if process_excel_file(excel_file):
            success_count += 1
        else:
            failed_count += 1
    
    # Итоговая статистика
    print()
    print("=" * 70)
    print("Итоги анализа:")
    print("=" * 70)
    print(f"✓ Успешно проанализировано: {success_count}")
    if failed_count > 0:
        print(f"✗ Ошибок: {failed_count}")
    print("=" * 70)


if __name__ == "__main__":
    main()
