# 💻2023-2학기 KOREATECH 데이터베이스시스템
### - 외부정렬 및 맵리듀스 구현

---
### 외부정렬


![image](https://github.com/user-attachments/assets/fd6708f6-d755-4182-a282-c9edbe4626c0)


### 맵리듀스

**컴파일 순서**


1. 두 개의 파일을 같은 디렉토리에 위치시키기
2. g++ -pthread –g –o wordcount wordcount.cpp 입력
3. wordcount 테스트용텍스트파일 입력 ex) ./wordcount part.tbl
4. result 파일 확인 ex) vi result


**진행단계**

1. to_binary() : MapReduce.cpp 79번 줄

![image](https://github.com/user-attachments/assets/e0e6a186-0b6c-46d3-a7ad-6a0febfcce81)

2. split() : MapReduce.cpp 99번 줄

![image](https://github.com/user-attachments/assets/2495ad8d-588f-4a02-b661-b4fd1ac183b0)

3. Map() : MapReduce.cpp 229번 줄 (세부 map_function() : 157번 줄)

![image](https://github.com/user-attachments/assets/b03eb13f-c71f-48d4-be39-dab9063fa86b)

4. partition() : MapReduce.cpp 302번 줄 (세부 partition_function() : 272번 줄)

![image](https://github.com/user-attachments/assets/4c56d79a-7ec5-4821-bf9e-6bc902dec39e)

5. external_sort() : MapReduce.cpp 470번 줄 (세부 sort_function(): 334번 줄)

![image](https://github.com/user-attachments/assets/4b4125ad-f5b0-4894-a1cc-202736a5fe74)

6. reduce() : MapReduce.cpp 519번 줄 (세부 reduce_function(): 494번 줄)

![image](https://github.com/user-attachments/assets/736dda19-998b-4caf-b845-8a8a02092174)

7. word_count_print() : MapReduce.cpp 562번 줄
 (세부 word_count_print_function(): 494번 줄)

![image](https://github.com/user-attachments/assets/7fa299b9-fc21-40fd-b7b3-2c14f547c3fd)





