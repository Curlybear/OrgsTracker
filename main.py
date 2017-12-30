import configparser
import requests
import datetime
import time
import json
import os
import psycopg2


def send_simple_message(subject, msg):
    return requests.post(
        "https://api.mailgun.net/v3/" + mail_domain + "/messages",
        auth=("api", mail_api_key),
        data={"from": "Orgs Tracker <orgstracker@" + mail_domain + ">",
              "to": [mail_to],
              "subject": subject,
              "html": msg})


def init_db(cursor):
    try:
        cursor.execute("""CREATE TABLE orgs (
                          org_id INT PRIMARY KEY,
                          country_id INT NOT NULL
                          );""")
        cursor.execute("""CREATE TABLE finances (
                          org_id INT,
                          date_capture DATE,
                          cc FLOAT,
                          gold FLOAT,
                          PRIMARY KEY (org_id,date_capture),
                          FOREIGN KEY (org_id)
                          REFERENCES orgs (org_id)
                          );""")

        orgs = json.loads(config.get("EREPD", "orgs"))

        orgStacks = [orgs[n:n + 10] for n in range(0, len(orgs), 10)]

        for orgStack in orgStacks:
            r = requests.get(
                'https://api.erepublik-deutschland.de/' + apiKey + '/organizations/details/' + ','.join(orgStack))
            if r.headers['X-Rate-Limit-Remaining'] == '0':
                time.sleep(float(r.headers['X-Rate-Limit-Reset']) + 5)

            obj = json.loads(r.text)
            for org in orgStack:
                try:
                    orgDetail = obj['organizations'][str(org)]
                    cc = orgDetail['money']['account']['cc']
                    gold = orgDetail['money']['account']['gold']
                    countryId = orgDetail['citizenship']['country_id']
                    cursor.execute("""INSERT INTO orgs(org_id, country_id) VALUES(%s, %s);""", (org, countryId))
                    cursor.execute("""INSERT INTO finances(org_id, date_capture, cc, gold) VALUES(%s, %s, %s, %s);""",
                                   (org, datetime.date.today(), cc, gold))
                except:
                    cursor.execute("""INSERT INTO orgs(org_id, country_id) VALUES(%s, %s);""", (org, -1))
                    cursor.execute("""INSERT INTO finances(org_id, date_capture, cc, gold) VALUES(%s, %s, %s, %s);""",
                                   (org, datetime.date.today(), -1, -1))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cursor.close()


def gather_data(cursor):
    try:
        orgs = json.loads(config.get("EREPD", "orgs"))

        orgStacks = [orgs[n:n + 10] for n in range(0, len(orgs), 10)]

        for orgStack in orgStacks:
            r = requests.get(
                'https://api.erepublik-deutschland.de/' + apiKey + '/organizations/details/' + ','.join(orgStack))
            if r.headers['X-Rate-Limit-Remaining'] == '0':
                time.sleep(float(r.headers['X-Rate-Limit-Reset']) + 5)

            obj = json.loads(r.text)
            for org in orgStack:
                try:
                    orgDetail = obj['organizations'][str(org)]
                    cc = orgDetail['money']['account']['cc']
                    gold = orgDetail['money']['account']['gold']
                    cursor.execute("""INSERT INTO finances(org_id, date_capture, cc, gold) VALUES(%s, %s, %s, %s);""",
                                   (org, datetime.date.today(), cc, gold))
                except:
                    cursor.execute("""INSERT INTO finances(org_id, date_capture, cc, gold) VALUES(%s, %s, %s, %s);""",
                                   (org, datetime.date.today(), -1, -1))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        cursor.close()


def analyze_data(cursor):
    try:
        cursor.execute("""
            WITH finance_changes(org_id, date_capture, current_cc, previous_cc, cc_diff) AS (
              SELECT
                org_id,
                date_capture,
                cc,
                lag(cc) OVER (w),
                cc - lag(cc) OVER (w)
              FROM finances
              WHERE cc >= 0
              WINDOW w AS (PARTITION BY org_id ORDER BY date_capture)
            )
            SELECT
              *
            FROM finance_changes
            WHERE
              date_capture = (SELECT max(date_capture) FROM finances)
              AND abs(cc_diff) > previous_cc / 5;
            """)
        rows = cursor.fetchall()

        content = """<table border="0" cellpadding="0" cellspacing="0" height="100%" width="100%" id="bodyTable">
                <tr>
                    <td align="left" valign="top">
                        <b>Variations greater than 20%</b>
                        <table border="1" cellpadding="10" cellspacing="0" width="500" id="emailContainer">
                            <tr>
                                <td>
                                    <b>Org id</b>
                                </td>
                                <td>
                                    <b>CC variation</b>
                                </td>
                            </tr>"""

        for row in rows:
            content += '<tr>'
            content += '<td>'
            content += str(row[0])
            content += '</td>'
            content += '<td>'
            content += str(row[4])
            content += '</td>'
            content += '</tr>'
        content += """</table>
                    </td>
                </tr>
            </table>"""

        cursor.execute("""
            WITH finance_changes(org_id, date_capture, current_cc, previous_cc, cc_diff) AS (
              SELECT
                org_id,
                date_capture,
                cc,
                lag(cc) OVER (w),
                abs(cc - lag(cc) OVER (w))
              FROM finances
              WHERE cc >= 0
              WINDOW w AS (PARTITION BY org_id ORDER BY date_capture)
            )
            SELECT
              *
            FROM finance_changes
            WHERE
              date_capture = (SELECT max(date_capture) FROM finances)
              AND (current_cc < -1 AND previous_cc > -1);
        """)

        rows = cursor.fetchall()

        content += """<table border="0" cellpadding="0" cellspacing="0" height="100%" width="100%" id="bodyTable">
                    <tr>
                        <td align="left" valign="top">
                            <b>Negative amounts of CC</b>
                            <table border="1" cellpadding="10" cellspacing="0" width="500" id="emailContainer">
                                <tr>
                                    <td>
                                        <b>Org id</b>
                                    </td>
                                    <td>
                                        <b>CC</b>
                                    </td>
                                </tr>"""

        for row in rows:
            content += '<tr>'
            content += '<td>'
            content += str(row[0])
            content += '</td>'
            content += '<td>'
            content += str(row[2])
            content += '</td>'
            content += '</tr>'
        content += """</table>
                        </td>
                    </tr>
                </table>"""

        send_simple_message("[OrgsTracker] Report - " + str(datetime.date.today()), content)
    except (Exception, psycopg2.DatabaseError) as error:
        print('gatherData - ' + error)
    finally:
        cursor.close()


# Config reader
directory = os.path.dirname(__file__)
filename = os.path.join(directory, 'config.ini')
config = configparser.ConfigParser()
config.read(filename)

# Config mail
mail_api_key = config['MAIL']['api_key']
mail_to = config['MAIL']['to']
mail_domain = config['MAIL']['domain']

# API Key
apiKey = config['EREPD']['api_key']

conn = None
cur = None
try:
    conn = psycopg2.connect(host=config['DB']['host'], database=config['DB']['db_name'], user=config['DB']['user'],
                            password=config['DB']['password'], port=config['DB']['port'])
    cur = conn.cursor()
    cur.execute("SELECT to_regclass('public.orgs');")
    res = cur.fetchone()
    if res[0] is None:
        init_db(conn.cursor())
    else:
        gather_data(conn.cursor())
        analyze_data(conn.cursor())
    conn.commit()

except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
    if cur is not None:
        cur.close()
