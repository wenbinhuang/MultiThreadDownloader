# coding: utf-8

import datetime
import requests
import re
import os
import threading
import codecs
import json
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

download_status_json = ''
download_status_dict = {}
DOWNLOAD_DIVIDE_SIZE_1M = 1024 * 1000
DOWNLOAD_DIVIDE_SIZE_10M = DOWNLOAD_DIVIDE_SIZE_1M * 10
DOWNLOAD_DIVIDE_SIZE_100M = DOWNLOAD_DIVIDE_SIZE_10M * 10


def get_time():
    return datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S.%f')[0:-3]


def pretty_print(msg):
    print('%s: %s' % (get_time(), msg))


def get_file_size(session, url):
    headers = {
        "Accept-Encoding": "identity",
        "Range": "bytes=0-1"
    }
    response = session.get(url, headers=headers, stream=True)
    # print(response.headers)
    content_range = response.headers.get('content-range')
    if content_range:
        try:
            # 'Content-Length': '2'
            # 'Content-Range': 'bytes 0-1/38523373'
            file_size = int(re.match(r'^bytes 0-1/(\d+)$', content_range).group(1))
        except:
            file_size = 0
    else:
        file_size = 0
    return file_size


def size_split(file_size, divide_num):
    average = file_size // divide_num
    result = []
    if file_size:
        for i in range(divide_num):
            if i != divide_num - 1:
                result.append((average * i, average * (i + 1) - 1, average))
            else:
                result.append((average * i, file_size - 1, average + file_size % divide_num))
    else:
        result.append((0, 0, 0))
    return result


def write_data(session_share, url, index, start, end, length, download_size, divide_size, file_w, file_w_lock, pbar, callback_func):
    info = {}
    divide_cnt = (length - download_size - 1) // divide_size + 1 if length > 0 else 1
    for cnt in range(divide_cnt):
        if length <= 0:
            headers_range = {'Range': f'bytes={start + download_size}-'}
        elif cnt != divide_cnt - 1:
            headers_range = {'Range': f'bytes={start+download_size}-{start+download_size+divide_size-1}'}
        else:
            headers_range = {'Range': f'bytes={start+download_size}-{end}'}
        res = session_share.get(url, headers=headers_range)
        file_w_lock.acquire()
        file_w.seek(start + download_size)
        file_w.write(res.content)
        file_w.flush()
        size = len(res.content)
        download_size += size
        pbar.update(size)
        info["download_total"] = download_size
        callback_func(index, info)
        file_w_lock.release()


def write_status(index, info):
    global download_status_json
    global download_status_dict
    download_status_dict["thread_status"][str(index)].update(info)
    with open(download_status_json, 'w') as f:
        json.dump(download_status_dict, f, indent=4)
        f.flush()


def check_change(file_size):
    global download_status_dict
    change = False
    if file_size == 0 or file_size != download_status_dict["file_size"]:
        change = True
    return change


def check_finish():
    global download_status_dict
    finish = False
    total = download_status_dict["thread_cnt"]
    download_size = 0
    for cnt in range(total):
        download_size += download_status_dict["thread_status"][f"{cnt}"]["download_total"]
    if download_size == download_status_dict["file_size"]:
        finish = True
    return finish


def download_split_data(session_share, url, index, download_info, file_w, file_w_lock, pbar, callback_func):
    start = download_info["data_start"]
    end = download_info["data_end"]
    length = download_info["data_length"]
    if length / DOWNLOAD_DIVIDE_SIZE_100M > 100:
        divide_size = DOWNLOAD_DIVIDE_SIZE_100M
    elif length / DOWNLOAD_DIVIDE_SIZE_10M > 10:
        divide_size = DOWNLOAD_DIVIDE_SIZE_10M
    else:
        divide_size = DOWNLOAD_DIVIDE_SIZE_1M
    download_size = 0 if not download_info.get("download_total") else download_info["download_total"]
    if download_size < length:
        write_data(session_share, url, index, start, end, length, download_size, divide_size, file_w, file_w_lock, pbar, callback_func)


def download(url, save_file_path=None, session=None):
    global download_status_json
    global download_status_dict
    cpu_max_thread_cnt = cpu_count()
    file_name = url.split('/')[-1] if save_file_path is None else save_file_path
    file_name_tmp = f"{file_name}.tmp"
    download_status_dict = {}
    download_status_json = f"{file_name}.json"
    session = requests.session() if session is None else session
    file_size = get_file_size(session, url)
    if not os.path.exists(file_name):
        if os.path.exists(file_name_tmp):
            if os.path.exists(download_status_json):
                try:
                    with codecs.open(download_status_json, 'r', 'utf-8') as f:
                        download_status_dict = json.load(f)
                except:
                    download_status = 0
                else:
                    if check_change(file_size):
                        download_status = 0
                    elif check_finish():
                        download_status = 2
                        pretty_print(f"{file_name}, already have downloaded.")
                    else:
                        pretty_print(f"{file_name}, downloaded un-complete last time.")
                        download_status = 1
            else:
                download_status = 0
        else:
            download_status = 0
    else:
        # download_status = 3
        pretty_print(f"{file_name}, already have downloaded.")
        return
    pretty_print(f"{file_name}, download now.")

    downloaded_size = 0
    if download_status == 0:
        file_w = open(f"{file_name_tmp}", 'wb')
        if file_size > 0:
            file_w.seek(file_size - 1)
            file_w.write(b'\0')
            # max multi-thread: x
            thread_cnt = cpu_max_thread_cnt * 3
        else:
            # single-thread: 1
            thread_cnt = 1
        pretty_print(f"thread_cnt={thread_cnt}")
        download_status_dict["file_url"] = url
        download_status_dict["file_name"] = file_name
        download_status_dict["file_size"] = file_size
        download_status_dict["thread_cnt"] = thread_cnt
        download_status_dict["thread_status"] = {}
        size_sp_list = size_split(file_size, thread_cnt)
        for index, size_sp in enumerate(size_sp_list):
            start_index, end_index, length = size_sp
            download_status_dict["thread_status"][str(index)] = {}
            download_status_dict["thread_status"][str(index)]["data_start"] = start_index
            download_status_dict["thread_status"][str(index)]["data_end"] = end_index
            download_status_dict["thread_status"][str(index)]["data_length"] = length
            download_status_dict["thread_status"][str(index)]["download_total"] = 0
    else:
        thread_cnt = download_status_dict["thread_cnt"]
        file_w = open(f"{file_name_tmp}", 'rb+')
        for index in range(thread_cnt):
            downloaded_size += download_status_dict["thread_status"][str(index)]["download_total"]

    file_w_lock = threading.Lock()
    with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024, desc=file_name,
              initial=downloaded_size) as pbar:
        with ThreadPoolExecutor(max_workers=cpu_max_thread_cnt*3//4) as executor:
            for index in range(thread_cnt):
                download_info = download_status_dict["thread_status"][str(index)]
                executor.submit(download_split_data, session, url, index, download_info, file_w, file_w_lock, pbar, write_status)
    file_w.close()
    os.rename(file_name_tmp, file_name)
    os.remove(download_status_json)
    pretty_print(f"{file_name} download complete.")

