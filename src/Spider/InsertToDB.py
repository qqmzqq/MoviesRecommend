import json
import sqlite3


def data_to_db(json_path):
    conn = sqlite3.connect("/home/cc/IdeaProjects/hadoop/src/webApp/db.sqlite3")
    with open(json_path, 'r') as f:
        data = json.load(f)
        for item in data:
            index = item.get('index')
            image = item.get('image')
            title = item.get('title')
            actor = item.get('actor')
            time = item.get('time')
            score = item.get('score')
            info = [index, image, title, actor, time, score]
            conn.execute('INSERT INTO '
                         'movies_movieinfo("index", image, title, actor, "time", score)'
                         'VALUES (?,?,?,?,?,?)', info)
            conn.commit()
    conn.close()


def recommended_to_db(file_path):
    conn = sqlite3.connect("/home/cc/IdeaProjects/hadoop/src/webApp/db.sqlite3")
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            info = line.strip('\n').split('\t')
            data = [info[0], info[1], info[2]]
            conn.execute('INSERT INTO movies_recommended(userId, "index", recommended)'
                         'VALUES (?,?,?)', data)
        conn.commit()
    conn.close()


if __name__ == '__main__':
    data_to_db('/home/cc/IdeaProjects/hadoop/data/MaoYan.json')
    recommended_to_db('/home/cc/IdeaProjects/hadoop/results/7-Top10/part-r-00000')
