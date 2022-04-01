
import fasttext

model = fasttext.load_model('fasttext_twitter_raw.bin')


def transform_w2v(x):
    w2v_list = []
    for i in x:
        if i in model:
            print(i)
            w2v_list.append(i)
    return w2v_list


def print_iter(x):
    print(x, type(x))
    for i in x:
        if i in model:
            print(i, type(i))
# df