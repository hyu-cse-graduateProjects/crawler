import glob

import requests
import re
from bs4 import BeautifulSoup
import csv
import time
from multiprocessing import Pool
import pandas as pd

output_columns = 'topic_id', 'topic', 'T/F', 'speecher', 'id', 'reference'

# topic_id : 각 분야
def crawl(topic_id):
    # flag 가 True 라면 최신팩트 url 형성
    url = 'http://factcheck.snu.ac.kr/v2/facts?page=' + str(1) + '&topic_id=' + str(topic_id)

    # 형성된 url 의 html 가져오기
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')

    # 각 분야의 주제에 해당하는 진위여부를 저장할 list
    results = []

    # 만들어진 url 과 html 이 제대로 만들어졌다면 계속 다음 page 로 가기
    while url and res:

        html = res.text

        # script 에서 필요한 내용인 id 와 score 를 위한 정규식을 작성한다.
        pattern = r'animate_meter\(\n +\{\n +\'id\': (.*?),\n +\'score\': \{\n +\'[a-z|A-Z|0-9|가-힣|(|)]+\' : (.*?),\n +\},\n +\'under_debate\':(.*?)\n +\}\n +\);'
        # html 에서 pattern 과 일치하는 내용을 뽑아낸다.
        matched = re.findall(pattern, html, re.S | re.MULTILINE)

        # 일치하는 내용의 수는 각 페이지에 존재하는 주제의 수와 같다. 이를 이용하여 각 주제의 세부 내용을 뽑아낸다.
        for i in range(0, len(matched)):
            id = int(matched[i][0].replace('\'', '').replace('id', '').strip())
            scores = matched[i][1].replace('\'', '').replace('\n', '').replace(',', '').replace(':', '').split()

            decided = True
            tf = True

            # score 값이 3이거나 0인 경우 진위여부가 판단되지 않았다 가정하여 decided 인자로 그것을 구분한다.
            # 그 외의 값인 5, 4인 경우 True 1, 2인 경우 False 로 거짓진실을 변수에 저장한다.
            for j in range(0, len(scores), 2):
                currentScore = int(scores[j])
                if currentScore == 0:
                    decided = False
                    break
                elif currentScore < 3:
                    tf = False
                elif currentScore > 3:
                    tf = True
                else:
                    decided = False

            # 진위여부가 판단 된 경우 내용, 말한 이, 출처를 html 에서 selector 를 이용해서 뽑아낸다.
            if decided:
                topic = soup.select('#container > div > div.left_article > div > ul > li:nth-child(' + str(i + 1) + ') > div > div.fcItem_top.clearfix > div.prg.fcItem_li > p:nth-child(3) > a')
                source = soup.select('#container > div > div.left_article > div > ul > li:nth-child(' + str(i + 1) + ') > div > div.fcItem_top.clearfix > div.person.fcItem_li > p')
                reference = soup.select('#container > div > div.left_article > div > ul > li:nth-child(' + str(i + 1) + ') > div > div.fcItem_top.clearfix > div.prg.fcItem_li > a')

                # 하나의 주제에서 발견한 모든 내용을 list 에 저장한다.
                results.append(crawlCsv(topic_id, tf, id, topic, source, reference))

        # 마지막 page 인 경우 next none 이라는 class 가 존재한다.
        nextNone = soup.find('a', {'class': 'next none'})

        # 단 하나의 page 인 경우 next 나 next none 이라는 class 둘다 존재 하지 않는다.
        tmpNext = soup.find('a', {'class': 'next'})

        # 현재 마지막 page 인 경우 nextNone 이 존재하므로 해당 분야를 끝내도록 한다.
        if nextNone:
            break

        # 하나의 page 만 있는 경우 다음 page 가 없으므로 해당 분야를 끝내도록 한다.
        elif not tmpNext:
            break

        # 다음 page 가 있는 경우 다음 page 로 가도록 한다.
        else:
            next = tmpNext.get('href')
            url = 'http://factcheck.snu.ac.kr' + next
            res = requests.get(url)
            soup = BeautifulSoup(res.content, 'html.parser')

    df = pd.DataFrame(results,columns=['topic_id', 'topic', 'T/F', 'speecher', 'id', 'reference'])
    df.to_csv(str(topic_id) + '.csv',index=False)


def crawlCsv(topic_id, tf, id, topic, source, reference):
    if not source:
        result_source = ""
    else:
        result_source = source[0].text.strip()

    if not reference:
        result_reference = ""
    else:
        result_reference = reference[0].text.strip()

    if not topic:
        result_topic = ""
    else:
        result_topic = topic[0].text.strip()

    return [topic_id, result_topic, tf, result_source, id, result_reference]


if __name__ == '__main__':
    start_time = time.time()

    # process 2개를 사용하여 crawling 을 진행한다.
    pool = Pool(processes=2)
    pool.starmap(crawl, zip(range(1, 8)))
    pool.close()

    finish_time = time.time()

    filelist = glob.glob("*.csv")
    # crawling 진행 시간을 print 한다.
    output = open(str(finish_time - start_time) + '.txt', 'w')
    output.close()

    OUTPUT = '서울대학교팩트체크_데이터.csv'

    result = pd.concat([pd.read_csv(f) for f in filelist], ignore_index=True, axis=0)

    if result.duplicated(subset=['id']).any():
        result[result['id'].duplicated()].to_csv('서울대학교팩트체크_중복.csv')

        result.drop_duplicates(subset='id',keep='first',inplace=True,ignore_index=True)
 
        result.to_csv(OUTPUT,index=False,encoding='utf-8')

    else:
        result.to_csv(OUTPUT, index=False, encoding='utf-8')