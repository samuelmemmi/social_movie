import csv
import mysql.connector


def credit(mydbb):
    with open('credits.csv', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        list_of_csv = list(csv_reader)

    j = 0
    mycursor = mydbb.cursor()
    actor_list = []
    caracter_list = []
    director_list = []
    id_list = []
    for li in list_of_csv:
        if li[0] != 'cast':
            name1 = li[0].split(",")
            strr1 = ""
            strr2 = ""
            for i in range(1, 17, 8):
                if i < len(name1):
                    first1 = name1[i].split("'character': ")
                    # print(first1)
                    second1 = first1[1].split("}")
                    strr1 += second1[0]
                    strr1 += ", "
            if strr1 != "":
                caracter_list.append(strr1)
            else:
                caracter_list.append("None")
            for i in range(5, 21, 8):
                if i < len(name1):
                    first2 = name1[i].split("'name': ")
                    # print(first2)
                    second2 = first2[1].split("}")
                    strr2 += second2[0]
                    strr2 += ", "
            if strr2 != "":
                actor_list.append(strr2)
            else:
                actor_list.append("None")

        if li[1] != 'crew':
            name2 = li[1].split(",")
            strr = ""
            if len(name2) > 1:
                first1 = name2[5].split("'name': ")
                # print(first1)
                second1 = first1[1].split("}")
                strr += second1[0]
                strr += ", "
            if strr != "":
                director_list.append(strr)
            else:
                director_list.append("None")

        if li[2] != 'id':
            id_list.append(li[2])
            j += 1

        if j == 1259:
            break

    for i in range(0, len(id_list)):
        sql1 = "INSERT INTO Actors (Actor_Name,Movie_ID, Caracter) VALUES (%s, %s, %s)"
        val1 = (actor_list[i], id_list[i], caracter_list[i])
        mycursor.execute(sql1, val1)
        mydbb.commit()
        sql2 = "INSERT INTO Directors (Director,Movie_ID) VALUES (%s, %s)"
        val2 = (director_list[i], id_list[i])
        mycursor.execute(sql2, val2)
        mydbb.commit()


def movie(mydbb):
    with open('movies_metadata.csv', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        list_of_csv = list(csv_reader)

    mycursor = mydbb.cursor()
    # 0
    adult_list = []
    # 2
    budget_list = []
    # 3
    genre_list = []
    # 5
    id_list = []
    # 7
    language_list = []
    # 8
    title_list = []
    # 9
    overview_list = []

    for li in list_of_csv:
        if li[0] != 'adult':
            adult_list.append(li[0])
        if li[2] != 'budget':
            budget_list.append(int(li[2]))
        if li[3] != 'genres':
            name = li[3].split(",")
            strr = ""
            for i in range(0, len(name)):
                if i % 2 != 0:
                    first = name[i].split("'name': ")
                    second = first[1].split("}")
                    strr += second[0]
                    strr += ", "
            if strr != "":
                genre_list.append(strr)
            else:
                genre_list.append("None")
        if li[5] != 'id':
            id_list.append(int(li[5]))
        if li[7] != 'original_language':
            language_list.append(li[7])
        if li[8] != 'original_title':
            title_list.append(li[8])
        if li[9] != 'overview':
            overview_list.append(li[9])

    for i in range(0, len(id_list)):
        sql1 = "INSERT INTO Movies_Data (Movie_ID,Budget,Adult,Title,Lang,Overview) VALUES (%s, %s, %s, %s,%s, %s)"
        val1 = (id_list[i], budget_list[i], adult_list[i], title_list[i], language_list[i], overview_list[i])
        mycursor.execute(sql1, val1)
        mydbb.commit()
    for i in range(0, len(id_list)):
        sql1 = "INSERT INTO Genres (Genre,Movie_ID) VALUES (%s, %s)"
        val1 = (genre_list[i], id_list[i])
        mycursor.execute(sql1, val1)
        mydbb.commit()


def keywordd(mydbb):
    with open('keywords.csv', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        list_of_csv = list(csv_reader)

    mycursor = mydbb.cursor()
    id_list = []
    key_list = []
    for li in list_of_csv:
        if li[0] != 'id':
            id_list.append(int(li[0]))
        name = li[1].split(",")
        strr = ""
        for i in range(0, len(name)):
            if i % 2 != 0:
                first = name[i].split("'name': ")
                second = first[1].split("}")
                strr += second[0]
                strr += ", "
        if strr != "":
            key_list.append(strr)
        else:
            if li[0] != 'id':
                key_list.append("None")

    for i in range(0, len(id_list)):
        sql = "INSERT INTO Keywords (Keyword, Movie_ID) VALUES (%s, %s)"
        val = (key_list[i], id_list[i])
        mycursor.execute(sql, val)
        mydbb.commit()


def rating(mydbb):
    with open('ratings.csv', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        list_of_csv = list(csv_reader)

    mycursor = mydbb.cursor()
    userid_list = []
    movieid_list = []
    rating_list = []
    for li in list_of_csv:
        if li[0] != 'userId':
            userid_list.append(int(li[0]))
        if li[1] != 'movieId':
            movieid_list.append(int(li[1]))
        if li[2] != 'rating':
            rating_list.append(float(li[2]))

    for i in range(0, 500000):
        sql = "INSERT INTO Ratings (User_ID,Movie_ID,Rating) VALUES (%s, %s, %s)"
        val = (userid_list[i], movieid_list[i], rating_list[i])
        mycursor.execute(sql, val)
        mydbb.commit()


if __name__ == "__main__":
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Raphael13@",
        database="movies"
    )

    keywordd(mydb)
    rating(mydb)
    movie(mydb)
    credit(mydb)
