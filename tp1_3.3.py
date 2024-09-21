import psycopg2 as db
from tabulate import tabulate


conn = db.connect(
    dbname='db3', # Mudar isso aquii 
    user='postgres',
    password='8146',          #vou mudar a senha aqui pq no meu bd botei outra senha 
    host='localhost',
    port='5432'
)

cursor = conn.cursor()

def func_db(escolha, cursor):
    query = None
    rows = None

    
    if escolha == 'A':  # OK
            
        respostaAsin = input("Insira o asin do produto desejado: ").strip()  # Remove espaços em branco
        # Tratar o caso no qual o usuario pediu um produto que foi desctoninuado
        asinQuery = '''
            SELECT asin, descontinuado
            FROM produto
            WHERE asin = %s;
        '''
        
        cursor.execute(asinQuery, (respostaAsin,))
        
        checkDescontinuado = cursor.fetchall()[0][1]
        # print(checkDescontinuado)
        if checkDescontinuado:
            print("Voce digitou um Produto Descontinuado!!!!!!!")
            return
        else:
            produtoAsin = respostaAsin

        if produtoAsin:
            produtoAsin = produtoAsin[0] if isinstance(produtoAsin, tuple) else produtoAsin  # Acessar o valor do asin dentro da tupla, se necessário
        else:
            print("Produto não encontrado")
            return  

    
        query = '''
            (
                SELECT *
                FROM Review
                WHERE asin = %s
                ORDER BY utilidade DESC, nota DESC
                LIMIT 5
            )
            UNION ALL
            (
                SELECT *
                FROM Review
                WHERE asin = %s 
                ORDER BY  nota ASC, utilidade DESC
                LIMIT 5
            );
        '''

        cursor.execute(query, (produtoAsin, produtoAsin))
        rows = cursor.fetchall()

        if rows:
            mais_uteis_maior_avaliacao = rows[:5]  # Os primeiros 5 comentários são mais úteis com maior avaliação
            mais_uteis_menor_avaliacao = rows[5:]  # Os próximos 5 são mais úteis com menor avaliação

            print("-------------------------------Os 5 comentários mais úteis e com maior avaliação-------------------------------")
            colunas = ['ASIN', 'Customer', 'Data', 'Rating', 'Votos', 'Utilidade']
            print(tabulate(mais_uteis_maior_avaliacao, headers=colunas, tablefmt="grid"))

            print()
            print("-------------------------------Os 5 comentários mais úteis e com menor avaliação-------------------------------")
            print(tabulate(mais_uteis_menor_avaliacao, headers=colunas, tablefmt="grid"))
            
        else:
            print("Nenhuma avaliação encontrada.")


    elif escolha == 'B': #OK


        respostaAsin = input("Insira o asin do produto desejado: ").strip()  # Remove espaços em branco
        # Tratar o caso no qual o usuario pediu um produto que foi desctoninuado
        asinQuery = '''
            SELECT asin, descontinuado
            FROM produto
            WHERE asin = %s;
        '''
        
        cursor.execute(asinQuery, (respostaAsin,))
        
        checkDescontinuado = cursor.fetchall()[0][1]
        if checkDescontinuado:
            print("Voce digitou um Produto Descontinuado!!!!!!!")
            return
        else:
            cod = respostaAsin

        query = f'''SELECT p_similar.asin, p_similar.titulo, p_similar.sales_rank
        FROM produto p
        JOIN similares s ON p.asin = s.asin
        JOIN produto p_similar ON s.asin_similar = p_similar.asin
        WHERE p.asin = '{cod}' AND p_similar.sales_rank < p.sales_rank;'''

        cursor.execute(query)

        rows = cursor.fetchall()

        if rows:

            colunas = ['ASIN_SIMILAR','TITULO','SALES_RANK']
            print(tabulate(rows, headers=colunas, tablefmt="grid"))
            
        else:
            print("Nenhuma avaliação encontrada.")
    
    
    
    elif escolha == 'C':

        respostaAsin = input("Insira o asin do produto desejado: ").strip()  # Remove espaços em branco
        # Tratar o caso no qual o usuario pediu um produto que foi descontinuado
        asinQuery = '''
            SELECT asin, descontinuado
            FROM produto
            WHERE asin = %s;
        '''
    
        cursor.execute(asinQuery, (respostaAsin,))
        
        checkDescontinuado = cursor.fetchall()[0][1]

        if checkDescontinuado:
            print("Voce digitou um Produto Descontinuado!!!!!!!")
            return
        else:
            cod = respostaAsin

        query = f'''SELECT
            CAST(data_review AS DATE) AS data_avaliacao,
            AVG(nota) AS media_avaliacao
        FROM
            review
        WHERE
            asin = '{cod}'
        GROUP BY
            data_avaliacao
        ORDER BY
            data_avaliacao;'''
        
        cursor.execute(query)

        rows = cursor.fetchall()

        #Caso o prodututo seja vazio 
        if not rows:
            print("O produto não recebeu nenhum review!")
        else:
            if rows:

                colunas = ['DATAS','MEDIA']
                print(tabulate(rows, headers=colunas, tablefmt="grid"))

        return
    

    elif escolha == 'D':  #OK    

        cursor.execute("SELECT DISTINCT grupo FROM produto")
        grupos = cursor.fetchall()

        for grupo in grupos:
            grupo_nome = grupo[0] 
            
            query = '''
                SELECT id, grupo, titulo, sales_rank
                FROM produto
                WHERE grupo = %s AND sales_rank > 0
                ORDER BY sales_rank ASC
                LIMIT 10;
            '''
            cursor.execute(query, (grupo_nome,))
            rows = cursor.fetchall()
            
            if rows:
                colunas = ['ID', 'Grupo', 'Título', 'Sales Rank']

                print(f"\nProdutos no grupo {grupo_nome}:")
                print(tabulate(rows, headers=colunas, tablefmt="grid"))
            else:
                print(f"\nNenhum produto encontrado no grupo {grupo_nome}.")


    elif escolha == 'E': #OK

        query = '''SELECT p.asin, p.titulo, AVG(r.utilidade) as media_uteis
                FROM produto p 
                JOIN review r ON p.asin = r.asin
                GROUP BY p.asin, p.titulo
                ORDER BY media_uteis DESC LIMIT 10;'''
        cursor.execute(query)

        rows = cursor.fetchall()

        if rows:

            colunas = ['ASIN','Titulo','AVG','Media Uteis']
            print(tabulate(rows, headers=colunas, tablefmt="grid"))
            
        else:
            print("Nenhuma avaliação encontrada.")

        
    elif escolha == 'F': #OK

        query = '''SELECT c.category_name, AVG(r.utilidade) as media_uteis
                FROM categoria c
                JOIN categoria_do_produto cp ON c.categoriaId = cp.categoriaId
                JOIN review r ON r.asin = cp.asin
                GROUP BY c.category_name
                ORDER BY media_uteis DESC LIMIT 5;'''
        cursor.execute(query)

        rows = cursor.fetchall()

        if rows:

            colunas = ['Nome Categoria','AVG','Media Uteis']
            print(tabulate(rows, headers=colunas, tablefmt="grid"))
            
        else:
            print("Nenhuma avaliação encontrada.")

    elif escolha == 'G': #OK
        query = '''SELECT grupo, cliente, num_reviews
                FROM (
                    SELECT p.grupo, r.cliente, COUNT(*) AS num_reviews,
                        ROW_NUMBER() OVER (PARTITION BY p.grupo ORDER BY COUNT(*) DESC) AS rn
                    FROM review r
                    JOIN produto p ON r.asin = p.asin
                    GROUP BY p.grupo, r.cliente
                ) sub
                WHERE rn <= 10
                ORDER BY grupo, num_reviews DESC;'''
        cursor.execute(query)
        
        rows = cursor.fetchall()
        
        if rows:

            colunas = ['GRUPO','CLIENTE','NUMERO DE REVIEWS']
            print(tabulate(rows, headers=colunas, tablefmt="grid"))
            
        else:
            print("não tem reviews.")



escolha  = None
while escolha != 'EXIT':

    print("""
Escolha uma das opções:\n
A : Dado um produto, listar os 5 comentarios mais uteis e com maior avaliaçao e os 5 comentarios mais uteis e com menor avaliaçao\n
B : Dado um produto, listar os produtos similares com maiores vendas do que ele\n
C : Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada\n
D : Listar os 10 produtos líderes de venda em cada grupo de produtos\n
E : Listar os 10 produtos com a maior média de avaliações úteis positivas por produto\n
F : Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto\n
G : Listar os 10 clientes que mais fizeram comentários por grupo de produto\n
EXIT : Sair 
    """)
    entrada = input("Digite uma letra (A, B, C, D, E, F, G) ou 'exit' para sair: ").lower()  # Converte para minúsculo para facilitar a verificação

    # Verifica se é uma das letras esperadas ou o comando 'exit'
    if entrada in ['a', 'b', 'c', 'd', 'e', 'f', 'g']:
        entrada = entrada.upper()  # Converte para maiúscula
        print(f"Você digitou: {entrada}")
    elif entrada == 'exit':
        print("Encerrando o programa...")
        break
    else:
        print("Entrada inválida. Por favor, digite A, B, C, D, E, F, G ou 'exit'.")
    func_db(entrada,cursor)
