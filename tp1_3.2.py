import psycopg2 as db
from psycopg2.extras import execute_values
import time

start_time = time.time()

# Conecta ao banco de dados
conn = db.connect(
    dbname='db3',  # Mudar para seu banco de dados
    user='postgres',
    password='123',
    host='localhost',
    port='5432'
)

cursor = conn.cursor()

# Cria as tabelas
cursor.execute('''
    CREATE TABLE IF NOT EXISTS categoria (
        categoriaId INTEGER PRIMARY KEY,
        category_name VARCHAR(155),
        parent_id INTEGER
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS produto (
        id SERIAL PRIMARY KEY,
        asin VARCHAR(15) UNIQUE,
        titulo VARCHAR(500),
        grupo VARCHAR(155),
        sales_rank INTEGER,
        descontinuado BOOL 
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS categoria_do_produto (
        asin VARCHAR(15),
        categoriaId INTEGER,
        PRIMARY KEY(asin, categoriaId),
        FOREIGN KEY(categoriaId) REFERENCES categoria(categoriaId),
        FOREIGN KEY(asin) REFERENCES produto(asin)
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS similares (
        asin VARCHAR(15),
        asin_similar VARCHAR(15),
        PRIMARY KEY(asin, asin_similar),
        FOREIGN KEY(asin) REFERENCES produto(asin)
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS review (
        id SERIAL,
        asin VARCHAR(15),
        cliente VARCHAR(20),
        data_review VARCHAR(55),
        nota INTEGER,
        votos INTEGER,
        utilidade INTEGER,
        PRIMARY KEY(id, cliente),
        FOREIGN KEY(asin) REFERENCES produto(asin)
    );
''')

# Confirmar a transação
conn.commit()

# Funções de processamento de arquivo
def process_file_categories(filename):
    categories = []
    with open(filename, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    in_categories_section = False
    for line in lines:
        line = line.strip()
        if line.startswith("categories:"):
            in_categories_section = True
            continue
        if in_categories_section:
            if line.startswith("reviews:"):
                in_categories_section = False
                continue
            parts = line.split('|')
            parent_id = None
            for part in parts:
                if '[' in part and ']' in part:
                    try:
                        name = part[:part.rfind('[')].strip()
                        id_str = part[part.rfind('[') + 1:part.rfind(']')].strip()
                        id = int(id_str)
                        categories.append((id, name, parent_id))
                        parent_id = id
                    except ValueError as e:
                        print(f"Erro ao processar a linha: {part}. Detalhes do erro: {e}")
    return categories

def process_file_review(file_name):
    reviews_dados = []
    asin = None
    situacao_review = False
    with open(file_name, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            line = line.strip()
            if line.startswith('ASIN:'):
                asin = line.split('ASIN:')[1].strip()
                situacao_review = False
            elif line.startswith('reviews:'):
                situacao_review = True
                continue
            if situacao_review and line and line[0].isdigit():
                try:
                    partes = line.split()
                    if len(partes) >= 9:
                        review_data = partes[0]
                        cliente = partes[2]
                        nota = int(partes[4])
                        votos = int(partes[6])
                        util = int(partes[8])
                        reviews_dados.append({
                            'asin': asin,
                            'data': review_data,
                            'id_cliente': cliente,
                            'nota': nota,
                            'votos': votos,
                            'util': util
                        })
                    else:
                        print(f"Linha inválida de review: {line}")
                except ValueError as e:
                    print(f"Erro ao processar a linha: {line}. Detalhes do erro: {e}")
    return reviews_dados

def process_file_categoria_produto(file_name):
    associations = []
    asin = None
    in_categories_section = False
    with open(file_name, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            line = line.strip()
            if line.startswith("ASIN:"):
                asin = line.split("ASIN:")[1].strip()
                in_categories_section = False
            elif line.startswith("categories:"):
                in_categories_section = True
                continue
            if in_categories_section:
                if line.startswith('|'):
                    parts = line.split('|')
                    for part in parts:
                        if '[' in part and ']' in part:
                            try:
                                category_id = int(part[part.rfind('[') + 1:part.rfind(']')])
                                associations.append((asin, category_id))
                            except ValueError as e:
                                print(f"Erro ao processar a linha: {part}. Detalhes do erro: {e}")
    return associations


def process_file_products(file_name):
    products = []
    asin = None
    title = None
    group = None
    salesRank = None
    discontinued = False
    with open(file_name, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        # print(lines) 
        for line in lines:
            line = line.strip()
            if line.startswith("ASIN"):
                if asin:
                    # Append the previous product
                    products.append((asin, title, group, salesRank, discontinued))
                asin = line.split("ASIN:")[1].strip()
                title = None
                group = None
                salesRank = None
                discontinued = False
            elif line == "discontinued product":
                # print(line)
                discontinued = True
            elif line.startswith("title"):
                title = line.split("title:", 1)[1].strip()
            elif line.startswith("group"):
                group = line.split("group:", 1)[1].strip()
            elif line.startswith("salesrank"):
                salesRank = line.split("salesrank:", 1)[1].strip()
        # Append the last product
        if asin:
            products.append((asin, title, group, salesRank, discontinued))
            
            
    for x in products:
        if x[4] == True:
            print(x)
    return products



def process_file_asin_similar(file_name):
    asin = None
    similar = []
    asin_similar_list = []
    with open(file_name, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            line = line.strip()
            if line.startswith("ASIN:"):
                if asin and similar:
                    asin_similar_list.append((asin, similar))
                asin = line.split("ASIN:")[1].strip()
                similar = []
            elif line.startswith("similar:"):
                similar = line.split("similar:")[1].strip().split()[1:]
        if asin and similar:
            asin_similar_list.append((asin, similar))
    return asin_similar_list

def check_asin_in_produto(asin_list, cursor):
    existing_asins = set()
    batch_size = 1000  # Você pode ajustar esse valor conforme necessário
    for i in range(0, len(asin_list), batch_size):
        batch = asin_list[i:i+batch_size]
        query = "SELECT asin FROM produto WHERE asin = ANY(%s)"
        cursor.execute(query, (batch,))
        result = cursor.fetchall()
        existing_asins.update(row[0] for row in result)
    missing_asins = set(asin_list) - existing_asins
    return missing_asins


file_name = "amazon-meta.txt"

categories = process_file_categories(file_name)
produtos = process_file_products(file_name)
categoria_produto = process_file_categoria_produto(file_name)
similar = process_file_asin_similar(file_name)
reviews = process_file_review(file_name)

# Preparação da lista de reviews
reviews_dados = [
    (
        review['asin'],
        review['id_cliente'],
        review['data'],
        int(review['nota']),
        int(review['votos']),
        int(review['util'])
    ) for review in reviews
]

batch_size = 1000  

print("Começando a inserir no banco...")

# Insere na tabela produto
query_produto = '''
    INSERT INTO produto (asin, titulo, grupo, sales_rank, descontinuado )
    VALUES %s
    ON CONFLICT (asin) DO NOTHING;
'''

for i in range(0, len(produtos), batch_size):
    batch = produtos[i:i+batch_size]
    execute_values(cursor, query_produto, batch)
    print(f"Produtos inseridos: {min(i + batch_size, len(produtos))} / {len(produtos)}")

# Insere na tabela categoria
query_categoria = '''
    INSERT INTO categoria (categoriaId, category_name, parent_id)
    VALUES %s
    ON CONFLICT (categoriaId) DO NOTHING;
'''

for i in range(0, len(categories), batch_size):
    batch = categories[i:i+batch_size]
    execute_values(cursor, query_categoria, batch)
    print(f"Categorias inseridas: {min(i + batch_size, len(categories))} / {len(categories)}")

# Verifica ASINs ausentes
asin_list_categoria_produto = [asin for asin, _ in categoria_produto]
missing_asins = check_asin_in_produto(asin_list_categoria_produto, cursor)

if missing_asins:
    print(f"ASINs ausentes na tabela produto: {missing_asins}")
else:
    print("Todos os ASINs estão presentes na tabela produto.")

# Insere na tabela categoria_do_produto
query_categoria_produto = '''
    INSERT INTO categoria_do_produto (asin, categoriaId)
    VALUES %s
    ON CONFLICT DO NOTHING;
'''

for i in range(0, len(categoria_produto), batch_size):
    batch = categoria_produto[i:i+batch_size]
    execute_values(cursor, query_categoria_produto, batch)
    print(f"Categoria do produto inserida: {min(i + batch_size, len(categoria_produto))} / {len(categoria_produto)}")

# Prepara os dados de similares
similar_data = [(asin, asin_similar) for asin, similares_list in similar for asin_similar in similares_list]

# Insere na tabela similares
query_similar = '''
    INSERT INTO similares (asin, asin_similar)
    VALUES %s
    ON CONFLICT DO NOTHING;
'''

for i in range(0, len(similar_data), batch_size):
    batch = similar_data[i:i+batch_size]
    execute_values(cursor, query_similar, batch)
    print(f"Similares inseridos: {min(i + batch_size, len(similar_data))} / {len(similar_data)}")

# Insere na tabela review
query_review = '''
    INSERT INTO review (asin, cliente, data_review, nota, votos, utilidade)
    VALUES %s
    ON CONFLICT DO NOTHING;
'''

for i in range(0, len(reviews_dados), batch_size):
    batch = reviews_dados[i:i+batch_size]
    execute_values(cursor, query_review, batch)
    print(f"Reviews inseridos: {min(i + batch_size, len(reviews_dados))} / {len(reviews_dados)}")

# Confirma a transação e fecha a conexão
conn.commit()
cursor.close()
conn.close()

print(f"Tempo de execução: {time.time() - start_time:.2f} segundos")
