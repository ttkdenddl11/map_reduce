#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <unordered_map>
#include <cstring>
#include <algorithm>
#include <time.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <algorithm>
#include <map>
#include <thread>
#include <mutex>
#include <string>

using namespace std;

#define SPLIT_BLOCK_SIZE 10000
#define MAP_BLOCK_SIZE 100000
#define MEMORY_SIZE 1800000


    struct RECORD {
        char str[256];
    } record;

    struct KeyValue {
        char key[64];
        int value;

        bool operator<(const KeyValue& a) const {
            return strcmp(key, a.key) < 0;
        }
    } keyvalue;

	struct RECORD records1[SPLIT_BLOCK_SIZE];
	struct RECORD records2[SPLIT_BLOCK_SIZE];
	struct RECORD records3[SPLIT_BLOCK_SIZE];
       	struct KeyValue maps1[MAP_BLOCK_SIZE];
	struct KeyValue maps2[MAP_BLOCK_SIZE];
	struct KeyValue maps3[MAP_BLOCK_SIZE];

class MapReduce{
private:
    FILE* lineitem_file;
    FILE* lineitem_bin_file;
    FILE* split_file1;
    FILE* split_file2;
    FILE* split_file3;
    FILE* map_file1;
    FILE* map_file2;
    FILE* map_file3;
    FILE* partition_file1;
    FILE* partition_file2;
    FILE* partition_file3;
    FILE* input1_file;
    FILE* input2_file;
    FILE* output1_file;
    FILE* output2_file;
    FILE* tmp_file;
    FILE* sort_file1;
    FILE* sort_file2;
    FILE* sort_file3;
    FILE* reduce_file1;
    FILE* reduce_file2;
    FILE* reduce_file3;
    FILE* result_file;


    double cpu_time_used;
    struct rusage r_usage;

    static mutex file_mutex;

    string input_file;

    void to_binary() {
        char buffer[256];

        lineitem_file = fopen(input_file.c_str(), "r");		// open lineitem.tbl
        if (lineitem_file == NULL) {
            printf("your_input_file open error\n");
            exit(0);
        }
        lineitem_bin_file = fopen("work", "wb");	// open lineitem.bin

        while (fgets(buffer, sizeof(buffer), lineitem_file)) {		// interval one line
            strcpy(record.str, buffer);
            if (record.str[0] == '\n')			// 공백 줄 예외 처리
                continue;
            fwrite(&record, sizeof(struct RECORD), 1, lineitem_bin_file);
        }
        fclose(lineitem_file);					// close file
        fclose(lineitem_bin_file);
    }

    void split() {
        lineitem_bin_file = fopen("work", "rb");	// 닫아주었던 변환 완료한 이진파일을 읽기모드로 연다.	
        if (lineitem_bin_file == NULL) {
            printf("lineitem.bin open error\n");
            exit(0);
        }
        split_file1 = fopen("work1", "wb");			// 문장 블록 크기(SPLIT_BLOCK_SIZE) 만큼씩 분배하기 위한 파일
        split_file2 = fopen("work2", "wb");
        split_file3 = fopen("work3", "wb");

        /* split 과정 */
        while (!feof(lineitem_bin_file)) {
            size_t x = 0;
            if ((x = fread(&records1[0], sizeof(struct RECORD), SPLIT_BLOCK_SIZE, lineitem_bin_file)) < SPLIT_BLOCK_SIZE) {
                if (x == 0)
                    break;

                fwrite(records1, sizeof(struct RECORD), x, split_file1);

                break;
            }
            else {
                // 초반에 x는 계속 RUN_SIZE 보다 클 것
                fwrite(records1, sizeof(struct RECORD), SPLIT_BLOCK_SIZE, split_file1);
            }

            if ((x = fread(&records2[0], sizeof(struct RECORD), SPLIT_BLOCK_SIZE, lineitem_bin_file)) < SPLIT_BLOCK_SIZE) {
                if (x == 0)
                    break;

                fwrite(records2, sizeof(struct RECORD), x, split_file2);

                break;
            }
            else {
                // 초반에 x는 계속 RUN_SIZE 보다 클 것
                fwrite(records2, sizeof(struct RECORD), SPLIT_BLOCK_SIZE, split_file2);
            }
            if ((x = fread(&records3[0], sizeof(struct RECORD), SPLIT_BLOCK_SIZE, lineitem_bin_file)) < SPLIT_BLOCK_SIZE) {
                if (x == 0)
                    break;

                fwrite(records3, sizeof(struct RECORD), x, split_file3);

                break;
            }
            else {
                // 초반에 x는 계속 RUN_SIZE 보다 클 것
                fwrite(records3, sizeof(struct RECORD), SPLIT_BLOCK_SIZE, split_file3);
            }
        }
        fclose(lineitem_bin_file);
        unlink("work");
        fclose(split_file1);
        fclose(split_file2);
        fclose(split_file3);
    }

    static void map_function(struct RECORD records[], FILE* split_file, FILE* map_file, struct KeyValue maps[], int i) {
        int m_size = 0;
        size_t x = 0;

        char delimiters[15] = " \t\n,.;:!?|"; // 여러 구분자 사용 가능

        while (1) {
            if ((x = fread(&records[0], sizeof(struct RECORD), SPLIT_BLOCK_SIZE, split_file)) < SPLIT_BLOCK_SIZE) {
                if (x == 0)
                    break;
                for (int i = 0; i < x; i++) {

                    char* token;
		    char *context;
                    KeyValue key_value;
                    // 첫 번째 토큰 분리
                    token = strtok_r(records[i].str, delimiters, &context);
                    // 토큰이 NULL이 될 때까지 반복
                    while (token != NULL) {
                        m_size++;

                        strcpy(maps[m_size - 1].key, token);
                        maps[m_size - 1].value = 1;

                        // 다음 토큰 분리

                        token = strtok_r(NULL, delimiters, &context);
                        if (m_size == MAP_BLOCK_SIZE) {
                            fwrite(maps, sizeof(KeyValue), m_size, map_file);
                            m_size = 0;
                        }
                    }
                }

                break;
            }
            else {
                for (int i = 0; i < SPLIT_BLOCK_SIZE; i++) {

                    char* token;
		    char* context;
                    KeyValue key_value;
                    // 첫 번째 토큰 분리
                    token = strtok_r(records[i].str, delimiters, &context);
                    // 토큰이 NULL이 될 때까지 반복
                    while (token != NULL) {
                        m_size++;

                        strcpy(maps[m_size - 1].key, token);
                        maps[m_size - 1].value = 1;

                        // 다음 토큰 분리
			
                        token = strtok_r(NULL, delimiters, &context);
                        if (m_size == MAP_BLOCK_SIZE) {
                            fwrite(maps, sizeof(KeyValue), m_size, map_file);
                            m_size = 0;
                        }
                    }

                }
            }
            if (feof(split_file)) {
                break;
	    }
        }

        if (m_size) {
            fwrite(maps, sizeof(KeyValue), m_size, map_file);
        }
    }

    void Map() {
        split_file1 = fopen("work1", "rb");
        if (split_file1 == NULL) {
            printf("split_file1 open error\n");
            exit(0);
        }
        split_file2 = fopen("work2", "rb");
        if (split_file2 == NULL) {
            printf("split_file2 open error\n");
            exit(0);
        }
        split_file3 = fopen("work3", "rb");
        if (split_file3 == NULL) {
            printf("split_file3 open error\n");
            exit(0);
        }
        map_file1 = fopen("work4", "wb");
        map_file2 = fopen("work5", "wb");
        map_file3 = fopen("work6", "wb");

        // multi thread 
        // 각 함수에 전달할 인자 설정
        thread t1(map_function, ref(records1), ref(split_file1), ref(map_file1), maps1, 1);
        thread t2(map_function, ref(records2), ref(split_file2), ref(map_file2), maps2, 2);
        thread t3(map_function, ref(records3), ref(split_file3), ref(map_file3), maps3, 3);
        t1.join();
        t2.join();
        t3.join();

	/* single thread
	map_function(records1, split_file1, map_file1, maps1, 1);
	map_function(records2, split_file2, map_file2, maps2, 2);
	map_function(records3, split_file3, map_file3, maps3, 3);
	*/

        fclose(split_file1);
        fclose(split_file2);
        fclose(split_file3);
        fclose(map_file1);
        fclose(map_file2);
        fclose(map_file3);
    }

    void partition_function(struct KeyValue maps[], FILE* map_file, FILE* partition_file1, FILE* partition_file2, FILE* partition_file3) {
        size_t x = 0;

        while (!feof(map_file)) {
            if ((x = fread(&maps[0], sizeof(struct KeyValue), MAP_BLOCK_SIZE, map_file)) < MAP_BLOCK_SIZE) {
                if (x == 0)
                    break;
                for (int i = 0; i < x; i++) {
                    if (maps[i].key[0] >= '0' && maps[i].key[0] <= '9')
                        fwrite(&maps[i], sizeof(KeyValue), 1, partition_file1);
                    else if (maps[i].key[0] >= 'a' && maps[i].key[0] <= 'z')
                        fwrite(&maps[i], sizeof(KeyValue), 1, partition_file2);
                    else
                        fwrite(&maps[i], sizeof(KeyValue), 1, partition_file3);
                }
                break;
            }
            else {
                for (int i = 0; i < MAP_BLOCK_SIZE; i++) {
                    if (maps[i].key[0] >= '0' && maps[i].key[0] <= '9')
                        fwrite(&maps[i], sizeof(KeyValue), 1, partition_file1);
                    else if (maps[i].key[0] >= 'a' && maps[i].key[0] <= 'z')
                        fwrite(&maps[i], sizeof(KeyValue), 1, partition_file2);
                    else
                        fwrite(&maps[i], sizeof(KeyValue), 1, partition_file3);
                }
            }
        }
    }

    void partition() {
        map_file1 = fopen("work4", "rb");
        if (map_file1 == NULL) {
            printf("map_file1 open error\n");
            exit(0);
        }
        map_file2 = fopen("work5", "rb");
        if (map_file2 == NULL) {
            printf("map_file2 open error\n");
            exit(0);
        }
        map_file3 = fopen("work6", "rb");
        if (map_file3 == NULL) {
            printf("map_file3 open error\n");
            exit(0);
        }
        partition_file1 = fopen("work1", "wb");
        partition_file2 = fopen("work2", "wb");
        partition_file3 = fopen("work3", "wb");

        partition_function(maps1, map_file1, partition_file1, partition_file2, partition_file3);
        partition_function(maps2, map_file2, partition_file1, partition_file2, partition_file3);
        partition_function(maps3, map_file3, partition_file1, partition_file2, partition_file3);

        fclose(map_file1);
        fclose(map_file2);
        fclose(map_file3);
        fclose(partition_file1);
        fclose(partition_file2);
        fclose(partition_file3);
    }

    void sort_function(FILE* partition_file, int check_sort_number) {
        input1_file = fopen("input1", "wb");
        input2_file = fopen("input2", "wb");

        /* ------ 내부 정렬 진행 과정 -----------*/

        while (!feof(partition_file)) {
            size_t x = 0;					// x : 구조체 크기를 RUN_SIZE 만큼 읽으라고 했는데 RUN_SIZE보다 작다면
                                            // 딱 그만큼만 정렬하고 입력파일에 써주기 위한 변수
            if ((x = fread(maps1, sizeof(struct KeyValue), MAP_BLOCK_SIZE, partition_file)) < MAP_BLOCK_SIZE) {
                if (x == 0)
                    break;
                sort(begin(maps1), begin(maps1) + x);
                fwrite(maps1, sizeof(struct KeyValue), x, input1_file);
                break;
            }
            else {								// 초반에 x는 계속 RUN_SIZE 보다 클 것 
                sort(begin(maps1), begin(maps1) + MAP_BLOCK_SIZE);
                fwrite(maps1, sizeof(maps1), 1, input1_file);
            }

            if ((x = fread(maps1, sizeof(struct KeyValue), MAP_BLOCK_SIZE, partition_file)) < MAP_BLOCK_SIZE) {
                if (x == 0)
                    break;
                sort(begin(maps1), begin(maps1) + x);
                fwrite(maps1, sizeof(struct KeyValue), x, input2_file);
                break;
            }
            else {                                                          // 초반에 x는 계속 RUN_SIZE 보다 클 것 
                sort(begin(maps1), begin(maps1) + MAP_BLOCK_SIZE);
                fwrite(maps1, sizeof(maps1), 1, input2_file);
            }

            getrusage(RUSAGE_SELF, &r_usage);				// 메모리 넘어가는지 체크
            if (r_usage.ru_maxrss > MEMORY_SIZE) {
                printf("메모리 사용량: %ld KB\n", r_usage.ru_maxrss);
                printf("MAP BLOCK SIZE를 줄이거나 메모리 크기를 늘려서 선언해주세요.\n");
                exit(0);						// 정해둔 메모리 넘어가면 탈출
            }
        }

        fclose(partition_file);
        fclose(input1_file);
        fclose(input2_file);

        input1_file = fopen("input1", "rb");	// 여기부터 2원균형합병을 위한 파일포인터들
        input2_file = fopen("input2", "rb");	// 서로 읽기파일과 쓰기파일을 번갈아가면서 맡게 될 파일포인터들
        output1_file = fopen("output1", "wb");
        output2_file = fopen("output2", "wb");

        /* ------------- 외부정렬 진행 과정 ---------- */

        int merge_run = MAP_BLOCK_SIZE;		// 병합하면서 run크기는 2배씩 증가할 예정이기 때문에 따로 정의해둔 상수를 변수로 설정해준다
        int file_index = 0;			// file_index : input파일들과 output파일들을 서로 읽기 쓰기 모드를 번갈아가며 하기 위한 체크변수

        while (fread(&keyvalue, sizeof(struct KeyValue), 1, input2_file) > 0) {		// 입력파일2의 내용이 비어있지 않은동안 반복하는 알고리즘
            fseek(input2_file, 0, SEEK_SET);					// 위에서 읽어서 확인했기 때문에 다시 0번 위치로 돌아간다.
            int index = 1;			// 쓸 파일을 정해주기 위한 체크변수
            int idx1 = 0, idx2 = 0;		// 외부 정렬시 필요한 변수
            KeyValue a, b;
            fread(&a, sizeof(struct KeyValue), 1, input1_file);			// 외부정렬
            fread(&b, sizeof(struct KeyValue), 1, input2_file);
            while (1) {
                if (index % 2 == 1)	// index가 홀수 일때는 output1_file에 써주기 위해
                    tmp_file = output1_file;
                else
                    tmp_file = output2_file;
                // 이 부분부터는 병합정렬의 알고리즘과 유사합니다.
                while (idx1 < merge_run && idx2 < merge_run && !feof(input1_file) && !feof(input2_file)) {
                    if (strcmp(a.key, b.key) < 0) {		// PARTKEY 기준으로 내부정렬 진행했기 때문에 외부정렬도 똑같이 PARTKEY 기준
                        fwrite(&a, sizeof(struct KeyValue), 1, tmp_file);
                        fread(&a, sizeof(struct KeyValue), 1, input1_file);
                        idx1++;
                    }
                    else {
                        fwrite(&b, sizeof(struct KeyValue), 1, tmp_file);
                        fread(&b, sizeof(struct KeyValue), 1, input2_file);
                        idx2++;
                    }
                }
                // 하나의 idx가 끝나거나 파일이 끝날 시 남아있는 부분들 처리
                while (!feof(input1_file) && idx1 < merge_run) {
                    fwrite(&a, sizeof(struct KeyValue), 1, tmp_file);
                    fread(&a, sizeof(struct KeyValue), 1, input1_file);
                    idx1++;
                }
                while (!feof(input2_file) && idx2 < merge_run) {
                    fwrite(&b, sizeof(struct KeyValue), 1, tmp_file);
                    fread(&b, sizeof(struct KeyValue), 1, input2_file);
                    idx2++;
                }
                index++;		// index : tmp_file을 뭐로 정할지 결정해주기 위한 체크카운트 변수(쓸 파일을 정해준다)
                idx1 = 0, idx2 = 0;	// 블록 크기만큼의 외부정렬을 끝내면 다시 0으로하고 아래쪽의 블록 크기만큼의 외부정렬을 진행
                if (feof(input1_file) && feof(input2_file))	// 블록크기만큼씩의 외부정렬을 모두 끝내면 종료 
                    break;					// 종료 후 새로운 input2_file을 읽으며 반복
            }
            merge_run *= 2;						// murge_run 크기는 두배씩 증가

            file_index++;

            if (file_index % 2) {		// file_index가 홀수면 output1을 읽을 파일로 열고
                freopen("output1", "rb", input1_file);
                freopen("output2", "rb", input2_file);
                freopen("input1", "wb", output1_file);
                freopen("input2", "wb", output2_file);
            }
            else {				// file_index가 짝수면 input1을 읽을 파일로 연다. 왜냐하면 홀수일때 input1을 쓰기로 열었기 때문
                freopen("input1", "rb", input1_file);
                freopen("input2", "rb", input2_file);
                freopen("output1", "wb", output1_file);
                freopen("output2", "wb", output2_file);
            }
        }					// 모두 끝나면 파일들 닫아준다.
        fclose(input1_file);
        fclose(input2_file);
        fclose(output1_file);
        fclose(output2_file);

        if (file_index % 2) { 			// 홀수면 output2에다가 쓴 상태이고 이것을 읽기로 열어서 빈파일이기 때문에 output1에는 다 쓰여져 있다 
            if (check_sort_number == 1)
                rename("output1", "sort1");
            else if (check_sort_number == 2)
                rename("output1", "sort2");
            else if (check_sort_number == 3)
                rename("output1", "sort3");
        }
        else {					// 짝수면 input2에다가 쓴 상태이고 이것을 읽기로 열어서 빈파일이기 때문에 input1에는 다 쓰여져 있다.
            if (check_sort_number == 1)
                rename("input1", "sort1");
            else if (check_sort_number == 2)
                rename("input1", "sort2");
            else if (check_sort_number == 3)
                rename("input1", "sort3");
        }
    }

    void external_sort() {
        partition_file1 = fopen("work1", "rb");	// 닫아주었던 변환 완료한 이진파일을 읽기모드로 연다.
        if (partition_file1 == NULL) {
            printf("partition_file1 open error\n");
            exit(0);
        }
        partition_file2 = fopen("work2", "rb");	// 닫아주었던 변환 완료한 이진파일을 읽기모드로 연다.
        if (partition_file2 == NULL) {
            printf("partition_file2 open error\n");
            exit(0);
        }
        partition_file3 = fopen("work3", "rb");	// 닫아주었던 변환 완료한 이진파일을 읽기모드로 연다.
        if (partition_file3 == NULL) {
            printf("partition_file3 open error\n");
            exit(0);
        }
        /* partition1 파일 정렬 */
        sort_function(partition_file1, 1);
        /* partition2 파일 정렬 */
        sort_function(partition_file2, 2);
        /* partition3 파일 정렬 */
        sort_function(partition_file3, 3);
    }

    static void reduce_function(struct KeyValue maps[], FILE* sort_file, FILE* reduce_file, int i) {
        int cnt = 1;
        KeyValue pre;
        if (fread(&pre, sizeof(struct KeyValue), 1, sort_file) != 1)
            return;

        KeyValue curr;

        while (!feof(sort_file)) {
            if (fread(&curr, sizeof(struct KeyValue), 1, sort_file) != 1)
                break;

            if (strcmp(pre.key, curr.key) == 0)
                cnt++;
            else {
                pre.value = cnt;
                fwrite(&pre, sizeof(struct KeyValue), 1, reduce_file);
                cnt = 1;
            }
            pre = curr;
        }
        curr.value = cnt;
        fwrite(&curr, sizeof(struct KeyValue), 1, reduce_file);
    }

    void reduce() {
        sort_file1 = fopen("sort1", "rb");
        if (sort_file1 == NULL) {
            printf("sort_file1 open error\n");
            exit(0);
        }
        sort_file2 = fopen("sort2", "rb");
        if (sort_file2 == NULL) {
            printf("sort_file2 open error\n");
            exit(0);
        }
        sort_file3 = fopen("sort3", "rb");
        if (sort_file3 == NULL) {
            printf("sort_file3 open error\n");
            exit(0);
        }
        reduce_file1 = fopen("work4", "wb");
        reduce_file2 = fopen("work5", "wb");
        reduce_file3 = fopen("work6", "wb");


	/* multi thread */
        thread t1(reduce_function, ref(maps1), ref(sort_file1), ref(reduce_file1), 1);
        thread t2(reduce_function, ref(maps2), ref(sort_file2), ref(reduce_file2), 2);
        thread t3(reduce_function, ref(maps3), ref(sort_file3), ref(reduce_file3), 3);
        t1.join();
        t2.join();
        t3.join();	
	
	/* single thread 
        reduce_function(maps1, sort_file1, reduce_file1, 1);
        reduce_function(maps2, sort_file2, reduce_file2, 2);
        reduce_function(maps3, sort_file3, reduce_file3, 3);
	*/

        fclose(sort_file1);
        fclose(sort_file2);
        fclose(sort_file3);
        fclose(reduce_file1);
        fclose(reduce_file2);
        fclose(reduce_file3);
    }

    void word_count_print_function(FILE* reduce_file, FILE* result_file) {
        while (!feof(reduce_file)) {
            size_t x = 0;					// x : 구조체 크기를 RUN_SIZE 만큼 읽으라고 했는데 RUN_SIZE보다 작다면
                                            // 딱 그만큼만 정렬하고 입력파일에 써주기 위한 변수
            if ((x = fread(maps1, sizeof(struct KeyValue), MAP_BLOCK_SIZE, reduce_file)) < MAP_BLOCK_SIZE) {
                if (x == 0)
                    break;

                for (int i = 0; i < x; i++) {
                    fprintf(result_file, "%s, %d\n", maps1[i].key, maps1[i].value);
                }
                break;
            }
            else {								// 초반에 x는 계속 RUN_SIZE 보다 클 것 
                for (int i = 0; i < MAP_BLOCK_SIZE; i++) {
                    fprintf(result_file, "%s, %d\n", maps1[i].key, maps1[i].value);
                }
            }
        }
    }

    void word_count_print() {
        reduce_file1 = fopen("work4", "rb");
        if (reduce_file1 == NULL) {
            printf("reduce_file1 open error\n");
            exit(0);
        }
        reduce_file2 = fopen("work5", "rb");
        if (reduce_file2 == NULL) {
            printf("reduce_file2 open error\n");
            exit(0);
        }
        reduce_file3 = fopen("work6", "rb");
        if (reduce_file3 == NULL) {
            printf("reduce_file3 open error\n");
            exit(0);
        }

	result_file = fopen("result", "w");

        word_count_print_function(reduce_file1, result_file);
        word_count_print_function(reduce_file2, result_file);
        word_count_print_function(reduce_file3, result_file);

        fclose(reduce_file1);
        fclose(reduce_file2);
        fclose(reduce_file3);

        unlink("work1");
        unlink("work2");
        unlink("work3");
        unlink("work4");
        unlink("work5");
        unlink("work6");
        unlink("sort1");
        unlink("sort2");
        unlink("sort3");
        unlink("input1");
        unlink("input2");
        unlink("output1");
        unlink("output2");
    }

public:
    MapReduce(string input_file) {
        this->input_file = input_file;
    }

    void run() {
        clock_t time_1, time_2;			// 시간을 체크하기 위한 변수

	time_1 = clock();

        to_binary();		// 데이터를 이진파일로 변환

        split();			// 문자열들 3개의 파일에 분배

        Map();				// map() 호출

        partition();		// partition() 호출

        external_sort();				// sort() 호출

        reduce();			// reduce() 호출

        word_count_print();

	time_2 = clock();

        getrusage(RUSAGE_SELF, &r_usage);	// 메모리 출력하기 위해 구조체를 받아오는 함수

        printf("실행 시간 : %f 초\n", ((double)(time_2 - time_1)) / CLOCKS_PER_SEC);
        printf("메모리 사용량: %ld KB\n", r_usage.ru_maxrss);
    }
};

mutex MapReduce::file_mutex;
