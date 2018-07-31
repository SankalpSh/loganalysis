import psycopg2


db_name = "news"


def main_func(query):

    """It connects to the database to run following queries"""
    conn = psycopg2.connect('dbname='+db_name)
    q = conn.cursor()
    q.execute(query)
    rows = q.fetchall()
    conn.close()
    return rows


def top_articles():

    """ returns top 3 articles """
    query1 = """
    SELECT articles.title, count(log.status) as count
    FROM articles
    JOIN log
    on log.status = '200 OK'
    where log.path like concat('/article/',articles.slug)
    GROUP BY articles.title
    ORDER BY count desc
    LIMIT 3;
    """
    # running query1
    results = main_func(query1)
    print ("\n The Top 3 articles :")
    k = 1
    for i in results:
        no = ' ('+str(k)+')'
        print no + " %s | with: %s views" % (i[0], i[1])
        k += 1


def top_authors():

    """ returns top 3 article authors """
    query2 = """
    SELECT authors.name , COUNT (log.status) as num
    FROM authors , log , articles
    where articles.author = authors.id
    and log.status = '200 OK'
    and log.path like concat('/article/%',articles.slug)
    GROUP BY authors.name
    ORDER BY num DESC
    LIMIT 3;
    """
    # running query2
    results = main_func(query2)
    print('\n The Top 3 article authors :')
    k = 1
    for i in results:
        no = ' ('+str(k)+')'
        print no + " %s | with: %s views" % (i[0], i[1])


def day_with_errors():

    """returns days with errors more than 1% """
    query3 = """
    SELECT error.date,
    ROUND(((error.error_status*1.0) / requests.get_status), 3) AS error_percent
    FROM (
    SELECT date_trunc('day', time) "date", count(*) AS error_status
    FROM log
    WHERE status LIKE '404%'
    GROUP BY date
    ) AS error
    JOIN (
    SELECT date_trunc('day', time) "date", count(*) AS get_status
    FROM log
    GROUP BY date
    ) AS requests
    ON error.date = requests.date
    WHERE (ROUND(((error.error_status*1.0) / requests.get_status), 3) > 0.01)
    ORDER BY error_percent DESC;
    """
    # running query3
    results = main_func(query3)
    print("\n Day with more than 1% errors :")
    for i in results:
        date = i[0].strftime('%B %d, %Y')
        error = str(round(i[1]*100, 1)) + "% errors"
        print " " + date + " " + error

if __name__ == "__main__":
    print " FETCHING INFORMATION ....\n"
top_articles()
top_authors()
day_with_errors()
