from random import randint


def get_index_list(number):
    index_list = []
    for num in range(1, number + 1):
        index = randint(1, 100)
        if index in index_list:
            continue
        else:
            index_list.append(index)
    return index_list


def make_data(out):
    for user_id in range(10001, 20001):
        index_list = get_index_list(randint(1, 100))
        for index in index_list:
            with open(out, 'a') as f:
                score = randint(0, 10)
                # print(str(user_id) + " " + str(index) + " " + str(score))
                f.write(str(user_id) + " " + str(index) + " " + str(score) + "\n")
        # print("---New User---")


if __name__ == '__main__':
    out_file = "D:\MyCode\MoviesRecommend\input\Matrix.txt"
    make_data(out_file)
