#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <vector>
#include <string.h>
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <algorithm>
using namespace std;

//used for storing words and their apparitions
typedef struct{
	string word; //the word
	vector <int> word_locations; //locations of the word
} word_details;

//used for the threads arguments
typedef struct{

	pthread_barrier_t *barrier; //barrier to make the reducers to start when the mapers finish their word
	pthread_barrier_t *barrier_2; //barrier to syncronize the reducers
	pthread_mutex_t *mutex;
	pthread_mutex_t (*mutexes_for_list_starting_with_a_character)[26];	//mutex for lists with the grouped words
	int thread_id;
	int *current_file_index;	//index of the current file
	vector <char *> file_names; //list of files that need to be read
	int num_files; //number of files that need to be read
	vector <word_details> *words; //list of words procesed by the current thread(partila lists)
	vector<vector<word_details>*> threads_words_lists; //used to pass the partial lists to all of the reducers
	size_t* current_partial_list_index; //index of the current partial list that is in proces to be united with the rest
	vector<vector<word_details>*> combinated_words_lists;
	int *index_for_sorting; //used to divide the the vectors ,with the grouped words, to be sorted
} thread_details; 

//comparator fucntion for the contents of the files
bool comparing_contents(const word_details& word1, const word_details& word2) {

    if (word1.word_locations.size() != word2.word_locations.size()) {
        return word1.word_locations.size() > word2.word_locations.size();
    }

    return word1.word < word2.word; 
}

//if a word is present in the word list
int in_word_list(vector <string> words, string word ){

	size_t  i = 0;
	for( i = 0; i < words.size(); i++){

        if (words[i] ==  word){
            return 1;
        }
    }
    return 0;

}

void *map(void *arg)
{
	thread_details arguments = *(thread_details *)arg; //the arguments
	int x = 1;
	char* current_file = NULL;	//the file that will be procesd by this process
	int file_number = -1;	//the number of the current file
	
	while (x == 1) {

		pthread_mutex_lock(arguments.mutex);

		//read from the list of files
		if ( (*arguments.current_file_index) < arguments.num_files) {
			
			current_file = arguments.file_names[*arguments.current_file_index];
			file_number = *arguments.current_file_index;
			file_number ++;
			(*arguments.current_file_index)++;
		}
		else {	//no remaining files to be procesed
			x = 0;
		}
		pthread_mutex_unlock(arguments.mutex);

		//read from the file line by line
		if (x != 0){
			
			FILE *file = fopen(current_file, "r");

			if (file == NULL) {

				return NULL;
			}

			char *line = NULL;
			size_t len = 0;
			vector <string> words_in_current_file; //list of words in the current file

			while (getline(&line, &len, file) != -1) {
				
				char *word;
				char *pointer_read;   //used to keep the position of the last read word
				word = strtok_r(line, " \n\t" ,&pointer_read);

				while ( word != NULL) {
					
					int nothing_left = 0; //after the elimination of the invalid characters check if something left
					string aux_word = word;

					//delete the invalid characters 
					for (long unsigned int aux = 0; aux < aux_word.size(); ) {

							if (aux_word[aux] == '\n' || aux_word[aux] == '\r') {
								aux_word.erase(aux, 1);
							}
							else if (aux_word[aux] >= 'A' && aux_word[aux] <= 'Z') {
								aux_word[aux] = aux_word[aux] + 32;
								aux++;
							}
							else if (aux_word[aux] < 'a' || aux_word[aux] > 'z') { 
								aux_word.erase(aux, 1);  
							} else {
								aux++;
							}
						}
						if (aux_word == ""){

							nothing_left = 1;
						}

					//add a new word to the list if the word isn't already present in the current file
					//  and if it's something left after the invalid characters are deleted
					if (in_word_list (words_in_current_file, aux_word) == 0 && nothing_left == 0){						
						
						word_details new_word;
						vector <int > word_location;

						new_word.word = aux_word;
						word_location.push_back(file_number);
						new_word.word_locations = word_location;
						words_in_current_file.push_back(new_word.word);
						arguments.words->push_back(new_word);						
					}
					word = strtok_r(NULL, " ", &pointer_read);
				}
				
			}
			free(line);
			fclose(file);
		}
	}
	
	pthread_barrier_wait(arguments.barrier); //the end of work for the mappers
	pthread_exit(NULL);
}

void *reduce(void *arg)
{	
	thread_details arguments = *(thread_details *)arg;
	pthread_barrier_wait(arguments.barrier); //reducers are waiting for the ending of the mappers

	int any_work = 1;
	vector<word_details> current_tread_word_list;

	//divide all the partials list of each mapper to the reducers
	while(any_work == 1){

		pthread_mutex_lock(arguments.mutex);

		if (*arguments.current_partial_list_index >= arguments.threads_words_lists.size()) {
        	any_work = 0; 
    	} 
		else {
			current_tread_word_list = (*arguments.threads_words_lists[*arguments.current_partial_list_index]);
			(*arguments.current_partial_list_index)++;
    	}
		pthread_mutex_unlock(arguments.mutex);
		
		if ((any_work == 1) ){
			
			//group the words starting with the same character in the same list
			for (long unsigned int i = 0; i < current_tread_word_list.size(); i++){
				
				word_details current_word = current_tread_word_list[i];
				int index = current_word.word[0] - 'a';
				pthread_mutex_lock(&(*arguments.mutexes_for_list_starting_with_a_character)[index]);
				vector<word_details> *list_in_which_to_place_current_word = arguments.combinated_words_lists[current_word.word[0] - 'a'];
				
				int verif = 0;

				for (size_t i = 0; i < list_in_which_to_place_current_word->size(); i++) {

					if ((*list_in_which_to_place_current_word)[i].word == current_word.word) {//if the word is in the list add only locations

						(*list_in_which_to_place_current_word)[i].word_locations.push_back(current_word.word_locations[0]);
						verif = 1;
						break;
					}
				}

				if (verif == 0){ //if the word isn't present in the list

					(*list_in_which_to_place_current_word).push_back(current_word);
				}
				pthread_mutex_unlock(&(*arguments.mutexes_for_list_starting_with_a_character)[index]);		
			}
		}
	}
	//waits to finish the grouping of words and agregation of the  partial files to start sorting and writing in files
	pthread_barrier_wait(arguments.barrier_2);

	int index_for_sroting = -1;
	int still_sorting = 1;

	//sorts all the grouped words in the descending order of number of files in which them apeared
	while(still_sorting == 1){
	
		pthread_mutex_lock(arguments.mutex);
		
		if ((*arguments.index_for_sorting) < 26){	//if something is left to be sorted
			index_for_sroting = (*arguments.index_for_sorting);
			(*arguments.index_for_sorting) ++;
		}
		else{
			still_sorting = 0;
			
		}
		pthread_mutex_unlock(arguments.mutex);
		
		if (still_sorting == 1){

			char create_file_letter = (char)(index_for_sroting + 'a');
			string new_file_name = string (1, create_file_letter);
			ofstream file_letter(new_file_name +".txt");

			//sorts the grouped words
			sort(arguments.combinated_words_lists[index_for_sroting]->begin(), arguments.combinated_words_lists[index_for_sroting]->end(), comparing_contents);
			vector<word_details> auxx_tread_word_list = *(arguments.combinated_words_lists[index_for_sroting]);

			//sorts the file locations in order
			for (long unsigned int m = 0; m < auxx_tread_word_list.size(); m++){
				
				int temp;
				for (long unsigned int l = 0; l < auxx_tread_word_list[m].word_locations.size() - 1; l++) {
					for (long unsigned int k = 0; k < auxx_tread_word_list[m].word_locations.size() - l - 1; k++) {

						if (auxx_tread_word_list[m].word_locations[k] > auxx_tread_word_list[m].word_locations[k + 1]) {
							temp = auxx_tread_word_list[m].word_locations[k];
							auxx_tread_word_list[m].word_locations[k] = auxx_tread_word_list[m].word_locations[k + 1];
							auxx_tread_word_list[m].word_locations[k + 1] = temp;
						}
					}
				}

				file_letter << auxx_tread_word_list[m].word<<":"<<"[";

				//write the the contents of the grouped and sorted words in the files
				for (long unsigned int n = 0 ; n < auxx_tread_word_list[m].word_locations.size(); n++){

					if ((n + 1) == auxx_tread_word_list[m].word_locations.size()) {

						file_letter << auxx_tread_word_list[m].word_locations[n] <<"]"<<endl;
					}
					else{
						file_letter <<auxx_tread_word_list[m].word_locations[n]<<" ";
					}
				}
			}
		}
	}

	pthread_exit(NULL);
}

int main(int argc, char **argv)
{   
    //test if the number of arguments is the correct one
    if (argc != 4) {

        printf("Insufficient arguments\n");
        return 1;
    }

    //reading from the command line arguments the number of mapper and reducer threads and the name of the file from which i got the 
	//other files
    int num_mapper_threads = atoi(argv[1]);
    int num_reducer_threads = atoi(argv[2]);
    char *input_file = argv[3];
    pthread_t mapper_threads[num_mapper_threads];
    pthread_t reducer_threads[num_reducer_threads];
    int i, r;
	void *status;
    thread_details arguments[num_mapper_threads + num_reducer_threads];
	pthread_barrier_t barrier; //used to syncronize the end of the mappers with starting of the reducers
	pthread_barrier_t barrier_2; //awaits for the end of grouping
	pthread_mutex_t mutex;
	pthread_mutex_t mutexes_for_list_starting_with_a_character[26];// mutexes for the grouped words vectors
	FILE *file_ptr;
	int num_files; //number of files that needs to be procesed
	int current_file_index = 0; //used to syncronize the reading from the files in order
	vector <word_details> words[num_mapper_threads]; //partial list of words that will be procesed of each thread
	vector <word_details> combinated_words_lists; 
	vector <word_details> words_that_star_with_this_character[26]; //the grouped words vectors
	long unsigned int current_partial_list_index = 0; //index of the current partial list that should be procesed by the reducer
	int index_for_sorting = 0;

	//read from the file given as an argument
	file_ptr = fopen(input_file, "r");

	if (file_ptr == NULL) {
		printf("File not found \n");
		return 1;
	}

	char *file_name_aux = NULL;
	size_t len_aux = 0;
	
	getline(&file_name_aux, &len_aux, file_ptr);
	
	//delete invalid characters
	if (file_name_aux[strlen(file_name_aux) - 1] == '\n'){

		file_name_aux[strlen(file_name_aux) - 1] = '\0';
		}

	while (file_name_aux[strlen(file_name_aux) - 1] == '\r'){

		file_name_aux[strlen(file_name_aux) - 1] = '\0';
	}

	num_files = stoi(file_name_aux);
	free(file_name_aux);

	vector <char *> file_names;

	for (i = 0; i < num_files; i++){
		
		char *file_name = NULL;
		size_t len = 0;
		getline(&file_name, &len, file_ptr);

		//delete invalid characters
		if (file_name[strlen(file_name) - 1] == '\n'){
			file_name [strlen(file_name) - 1] = '\0';
		}

		if (file_name[strlen(file_name) - 1] == '\r') {
			file_name [strlen(file_name) - 1] = '\0';
		}

		file_names.push_back(file_name);
	}
	fclose(file_ptr);

	//initialize the barrier to syncronyze the end of all the mapper threads with the starting of the reducers
	//in this way i wait to finish the MAP, after that i start that REDUCE aspect
	pthread_barrier_init(&barrier, NULL, num_mapper_threads + num_reducer_threads);
	pthread_barrier_init(&barrier_2, NULL, num_reducer_threads);
	pthread_mutex_init(&mutex, NULL);

	for (int i = 0; i < 26; ++i) {
		pthread_mutex_init(&mutexes_for_list_starting_with_a_character[i], NULL);
	}
	
    for (i = 0; i < num_mapper_threads + num_reducer_threads; i++) {

		arguments[i].barrier = &barrier;
		arguments[i].barrier_2 = &barrier_2;
		arguments[i].mutex = &mutex;
		arguments[i].thread_id = i;
		arguments[i].current_file_index = &current_file_index;
		arguments[i].file_names = file_names;
		arguments[i].num_files = num_files;
		arguments[i].mutexes_for_list_starting_with_a_character = &mutexes_for_list_starting_with_a_character;

		if (i < num_mapper_threads){	//create and run the mapper threads

			arguments[i].words = &words[i];
			r = pthread_create(&mapper_threads[i], NULL, map, &arguments[i]);
		}
		else {	//create and run the reducer treads threads
			arguments[i].index_for_sorting = &index_for_sorting;
			arguments[i].words = &combinated_words_lists;
			arguments[i].current_partial_list_index = &current_partial_list_index;

			//pass the partial lists tho the reducer threads
			for (int j = 0; j < num_mapper_threads; j++){

				arguments[i].threads_words_lists.push_back(&words[j]);
			}

			//pass the partial mutexes for grouped words tho the reducer threads
			for (int j = 0; j < 26; j++){
				arguments[i].combinated_words_lists.push_back(&words_that_star_with_this_character[j]);
				
			}
			r = pthread_create(&reducer_threads[i - num_mapper_threads], NULL, reduce, &arguments[i]);
		}

		if (r) {
			printf("Eroare la crearea thread-ului %d\n", i);
			exit(-1);
		}
	}

	for (i = 0; i < num_mapper_threads + num_reducer_threads; i++) {

		if (i < num_mapper_threads)
			r = pthread_join(mapper_threads[i], &status);
		else {
			r = pthread_join(reducer_threads[i - num_mapper_threads], &status);
		}

		if (r) {
			printf("Eroare la asteptarea thread-ului %d\n", i);
			exit(-1);
		}
	}
		//free the memory alocated for the files
	for (i = 0; i < num_files; i++){
		free(file_names[i]);
	}
	pthread_barrier_destroy(&barrier);
	pthread_mutex_destroy(&mutex);
    return 0;
}