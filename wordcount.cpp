#include "MapReduce.cpp"

int main(int argc, char** argv) {
	
	if(argc != 2) {
		printf("follow the execution format [./wordcount file_name]");
		return 0;
	}

	MapReduce wordcount(argv[1]);
	wordcount.run();

	return 0;
}
