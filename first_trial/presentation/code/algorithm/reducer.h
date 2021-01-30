//
// Created by enaki on 12.01.2021.
//

#ifndef MPI_MAP_REDUCE_PROJECT_REDUCER_H
#define MPI_MAP_REDUCE_PROJECT_REDUCER_H

#include "shared.h"
#include "../utils/word_parser.h"

void reduce_direct_index_phase(std::vector<std::string>* filenames_ptr, const char* filename, int rank){
    std::unordered_map<std::string, unsigned long long> word_map;

    std::vector<std::string> word_vector;
    for (auto & temp_filename : *filenames_ptr){
        word_parser::split_string_by_separator(word_vector, temp_filename, '@');
        auto compare_result = strcmp(filename, word_vector[0].c_str());
        if (compare_result == 0){
            word_parser::update_word_dict(word_map, word_vector[1], std::stoull(word_vector[2]));
        } else if (compare_result < 0){
            break;
        }
        word_vector.clear();
    }
    printer::create_word_files_from_dictionary(TEMP_FOLDER_DIRECT_IDX_PHASE_REDUCERS, word_map, filename, rank);
}

void reduce_indirect_index_phase(std::vector<std::string>* filenames_ptr, const char* word){
    std::list<PAIR_STRING_COUNT> word_list_with_file_pair;
    std::vector<std::string> word_vector;
    for (auto & temp_filename : *filenames_ptr){
        word_parser::split_string_by_separator(word_vector, temp_filename, '@');
        auto compare_result = strcmp(word, word_vector[0].c_str());
        if (compare_result == 0){
            word_list_with_file_pair.emplace_back(std::make_pair(word_vector[1], std::stoull(word_vector[2])));
        } else if (compare_result < 0){
            break;
        }
        word_vector.clear();
    }
    printer::create_word_files_from_filenames_pair(TEMP_FOLDER_INDIRECT_IDX_PHASE_REDUCERS, word_list_with_file_pair, word);
}

#endif //MPI_MAP_REDUCE_PROJECT_REDUCER_H
