import pymysql # 전처리

conn = pymysql.connect(host='localhost', user='root', passwd='!CLT-c403s', charset='utf8')
cursor = conn.cursor()

def get_cnt():
    q1 = f"""USE car"""
    q2 = f"""SELECT COUNT(*) as cnt FROM total"""

    cursor.execute(q1)
    cursor.execute(q2)
    cnt = cursor.fetchall()[0][0]

    conn.commit()
    conn.close()

    return cnt