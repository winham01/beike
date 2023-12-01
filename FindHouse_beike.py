import re
import requests
from bs4 import BeautifulSoup
import time
import datetime
import pymysql
import random
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import fake_useragent


class FindHouse:

    def __init__(self):
        self.proxies_list = []
        self.head_list = []
        self.city = None
        print("我要开始面向对象啦！")

    @staticmethod
    def get_proxies():
        base_url = 'http://www.kxdaili.com/dailiip/1/'
        # base_url = 'http://www.66ip.cn/'
        writer = pd.ExcelWriter("result.xlsx")
        for i in range(1, 11):
            url = base_url + str(i) + '.html'
            data = pd.read_html(url)[0]
            data = pd.DataFrame(data)
            data.to_excel(writer, startrow=(i - 1) * 10, index=False, header=False)
        writer.close()

    def get_proxies_list(self):
        data = pd.read_excel("result.xlsx")
        data_arr = np.asarray(data.stack())  # Dataframe类型堆叠变成Series类型再转成numpy数组
        data_list = data_arr.tolist()  # 再将数组转换成list
        tem_list = []
        for i in range(99):
            tem_list.append(data_list[i * 7:i * 7 + 6])
        for proxy in tem_list:
            proxy = {'http': f'{str(proxy[0]) + ":" + str(proxy[1])}'}
            self.proxies_list.append(proxy)

        # print(self.proxies_list)
        return self.proxies_list

    def get_head_list(self):
        ua = fake_useragent.UserAgent()
        self.head_list = []
        for i in range(100):
            self.head_list.append(ua.chrome)
        for j in range(100):
            self.head_list.append(ua.edge)
        for k in range(100):
            self.head_list.append(ua.ie)
        for h in range(100):
            self.head_list.append(ua.firefox)

        # print(self.head_list)
        return self.head_list

    def get_html(self, url):
        head = {'User-Agent': self.head_list[random.randint(0, len(self.head_list) - 1)]}
        proxy = self.proxies_list[random.randint(0, len(self.proxies_list) - 1)]
        html = requests.get(url, headers=head, proxies=proxy)
        # print(html)
        # print(proxy, head)
        return html

    def get_city_url(self):
        """
        获取贝壳网所有城市和城市首页的url字典
        :return: 城市和城市首页url的字典
        """
        baseurl = 'https://www.ke.com/city/'
        city_html = self.get_html(baseurl)
        soup = BeautifulSoup(city_html.text, "html.parser").find_all("li", class_='CLICKDATA')
        city_url_dict = {}
        for item in soup:
            each_city_url_text = re.findall(re.compile(r'a href="(.*?)"'), str(item))[0]
            each_city_cn_name = re.findall(re.compile(r'a href=".*?">(.*?)</a>'), str(item))[0]
            each_city_url = 'https:%s' % each_city_url_text
            city_url_dict[each_city_cn_name] = each_city_url

        # print(city_url_dict)
        return city_url_dict

    def get_district_url(self, city_url):
        """
        根据城市首页地址获取不同的区
        :param city_url: 城市首页的url
        :return: 该城市的所有区url列表
        """
        city_home_url = '%s/ershoufang' % city_url  # 这里是为了兼容下面的区url
        district_html = self.get_html(city_home_url)
        soup_1 = BeautifulSoup(district_html.text, 'html.parser')
        soup_2 = soup_1.find("div", class_="position").find_all("a", class_="CLICKDATA")
        district_url_list = []
        for item in soup_2:
            # print(item.text,item.get("href"))
            district_url = city_url + item.get("href")
            district_url_list.append(district_url)

        # print(district_url_list)
        return district_url_list

    @staticmethod
    def get_room_floor_url_list(district_url):
        """
        按照不同的搜索维度将该区的房源划分成不同的网页模块
        :param district_url: 区url
        :return: 当前区不同居室，不同楼层，不同翻页的url列表
        """
        room_list = ['l1', 'l2', 'l3', 'l4', 'l5', 'l6']  # 1室，2室，3室，4室，5室，5室以上
        floor_list = ['lc1', 'lc2', 'lc3', 'lc4', 'lc5']  # 低层，中层，高层，底层，顶层
        room_floor_url_list = []
        for each_room in room_list:
            for each_floor in floor_list:
                room_floor_url_list.append(district_url + each_room + each_floor)

        # print(room_floor_url_list)
        return room_floor_url_list  # 返回所有网页的url

    def get_page_url_list(self, room_floor_url):
        """
        根据输入的居室楼层url，获取当前居室/楼层下的所有翻页房源url
        :param room_floor_url: 特定城市/区/居室/楼层的url
        :return: 当前居室/楼层下的所有翻页房源url
        """
        room_floor_html = self.get_html(room_floor_url)
        page_url_list = []
        page_text = re.findall(re.compile(r'"totalPage":(.*?),'), room_floor_html.text)
        # total_quantity = re.findall(re.compile(r'共找到<span>(.*?)</span>套'), html.text)[0]
        if page_text:
            page_quantity = int(page_text[0])
            for page in range(1, page_quantity + 1):
                page_url_list.append(room_floor_url + 'pg' + str(page))
            # print(page_quantity)
        else:
            print('当前城市_区_居室_楼层下没有房源：%s' % room_floor_url)
            pass

        # print(page_url_list)
        return page_url_list

    def get_house_url_list(self, page_url):
        """
        获取具体一个城市/区/居室/楼层/翻页的房源url，一般一个页面是30个房源
        :param page_url: 具体页面数的url
        :return: 当前页面中所有房源的url列表，一般为固定30个
        """
        page_html = self.get_html(page_url)
        house_url_list = []
        soup = BeautifulSoup(page_html.text, "html.parser")
        for item in soup.find_all("div", class_="info clear"):
            house_url = re.findall(re.compile(r'href="(.*?)" target="_blank"'), str(item))
            house_url_list.append(house_url[0])

        # print(house_url_list)
        return house_url_list

    def get_house_info(self, house_url):
        """
        根据输入的房源url获取网页信息内容
        :param house_url: 具体一个房源的url
        :return: 房源的信息列表
        """
        house_html = self.get_html(house_url)
        soup_1 = BeautifulSoup(house_html.text, "html.parser")
        soup = soup_1.find("div", class_="overview").find("div", class_="content")
        if soup.find("span", class_="total") is not None:
            total_price = soup.find("span", class_="total").string
            unit_price = soup.find("span", class_="unitPriceValue").string
        else:
            text = soup.find("div", class_="priceBox").text
            total_price = re.findall(re.compile(r'本套房源的参考总价:(.*?)万'), text)[0]
            unit_price = re.findall(re.compile(r'本小区政府参考单价:(.*?)元'), text)[0]
        room = soup.find("div", class_="room").find("div", class_="mainInfo").string
        floor = soup.find("div", class_="room").find("div", class_="subInfo").string
        direction = soup.find("div", class_="type").find("div", class_="mainInfo").string
        decoration = soup.find("div", class_="type").find("div", class_="subInfo").string
        area = soup.find("div", class_="area").find("div", class_="mainInfo").string.replace('平米', '')
        year = soup.find("div", class_="area").find("div",
                                                    class_="subInfo noHidden")  # .replace('\n', '').replace(' ', '')
        year = re.findall(re.compile('noHidden">(.*?)\n', re.S), str(year))[0]
        location = soup.find("a", class_="info no_resblock_a").string
        title = soup_1.find("div", class_="title").find("h1", class_="main").string.replace('\n', '').replace(' ', '')
        district = soup.find("div", class_="areaName").find("a").string
        follower = soup_1.find("div", class_="btnContainer").find("span", class_="count").string
        sale_time = soup_1.find("div", class_="transaction").find("li", class_='').text
        sale_time = sale_time.replace('\n', '').replace(' ', '').replace('挂牌时间', '')
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        house_info = (0, current_time,
                      house_url, title, self.city, district, float(follower),
                      float(total_price), float(unit_price), float(area),
                      room, floor, direction, decoration, year, location, sale_time
                      )

        # print(house_info)
        return house_info

    @staticmethod
    def write_data(house_info):
        """
        将爬下来的数据写入mysql数据库
        :param house_info: 房源信息列表
        :return: 无
        """
        conn = pymysql.connect(host="localhost", user="root", passwd="123456", db='house_info', charset='utf8',
                               port=3306)
        cursor = conn.cursor()
        # sql_insert = f"insert into test_data values{house_info}"  # 第一种格式化方法
        # sql_insert = "insert into zhuzhou values%s"%str(house_info)  # 第二种格式化方法
        sql_insert = "insert into test_data values{}".format(house_info)  # 第三种格式化方法
        cursor.execute(sql_insert)
        conn.commit()

    def input_city(self):
        """
        输入窗口函数，用来接受用户输入的查询城市
        :return: 城市
        """
        city_url_dict = self.get_city_url()  # 类内调用方法
        while True:
            self.city = input('请输入想要查询的城市：')
            if self.city in city_url_dict and 'fang' not in city_url_dict[self.city]:
                break
            else:
                print('数据库中不存在该城市，请重新输入！')
                continue
        city_url = city_url_dict[self.city]

        print(city_url)
        return city_url

    @staticmethod
    def input_quantity():
        """
        输入窗口函数，用来接受用户输入的查询数量
        :return: 数量
        """
        while True:
            data = input('请输入要查询的数量：')
            try:
                if type(eval(data)) == int and eval(data) > 0:
                    break
                else:
                    print("请输入正整数！")
                    continue
            except Exception:
                print('请输入正整数！')
                continue
        quantity = eval(data)

        # print(quantity)
        return quantity


# 单线程
def main_solo():
    my_case = FindHouse()  # 实例化一个对象my_case
    my_case.get_proxies_list()  # 重新赋值实例属性代理列表
    my_case.get_head_list()
    city_url = my_case.input_city()  # 获取输入的城市，对应的城市首页url
    quantity = my_case.input_quantity()  # 获取输入的数量
    count = 0
    start_time = time.time()
    while count < quantity:
        district_url_list = my_case.get_district_url(city_url)  # 获取区url列表
        for district_url in district_url_list:
            room_floor_url_list = my_case.get_room_floor_url_list(district_url)  # 获取居室_楼层下的url列表
            for room_floor_url in room_floor_url_list:
                page_url_list = my_case.get_page_url_list(room_floor_url)  # 获取所有翻页的url列表
                for page_url in page_url_list:
                    house_url_list = my_case.get_house_url_list(page_url)  # 获取所有单个房源的url列表
                    for house_url in house_url_list:
                        try:
                            house_info = my_case.get_house_info(house_url)  # 获取单个房源的信息
                            my_case.write_data(house_info)
                            end_time = time.time()
                            use_time = end_time - start_time
                            count += 1
                            print('正在写入第:%d 条数据，已耗时：%s秒' % (count, use_time))
                        except Exception as e:
                            count += 1
                            print('当前数据不符合规范，跳过当前数据：{}'.format(house_url))
                            print(e)
                        if count >= quantity:
                            print("查询完成^_^")
                            return


# 多线程
def main_multi():
    my_case = FindHouse()  # 实例化一个对象my_case
    my_case.get_proxies_list()  # 重新赋值实例属性代理列表
    my_case.get_head_list()  # 重新赋值实例属性请求头列表
    city_url = my_case.input_city()  # 获取输入的城市，对应的城市首页url
    quantity = my_case.input_quantity()  # 获取输入的数量
    count = 0
    t1 = time.time()
    while count < quantity:
        district_url_list = my_case.get_district_url(city_url)  # 获取区url列表
        for district_url in district_url_list:
            room_floor_url_list = my_case.get_room_floor_url_list(district_url)  # 获取居室_楼层下的url列表
            # print('获取该城市所有居室_楼层url', room_floor_url_list)
            for room_floor_url in room_floor_url_list:
                page_url_list = my_case.get_page_url_list(room_floor_url)  # 获取所有翻页的url列表
                # print('获取该居室_楼层下所有翻页url', page_url_list)
                for page_url in page_url_list:
                    house_url_list = my_case.get_house_url_list(page_url)
                    # print('获取该页面下所有房源url', house_url_list)

                    with ThreadPoolExecutor(max_workers=10) as pool:  # 创建线程池
                        for house_info in pool.map(my_case.get_house_info, house_url_list):
                            try:
                                my_case.write_data(house_info)
                                count += 1

                                print(f'正在写入第:{count}条数据')
                            except Exception as e:
                                count += 1
                                print('当前数据不符合规范，跳过当前数据：{}'.format(house_url_list[count]))
                                print(e)
                            if count >= quantity:
                                t2 = time.time()
                                print('总耗时：%s秒' % (t2 - t1))
                                print("查询完成^_^")
                                return


if __name__ == '__main__':
    # main_solo()
    main_multi()
