//
// Created by enaki on 12.01.2021.
//

#ifndef MPI_MAP_REDUCE_PROJECT_REDUCER_H
#define MPI_MAP_REDUCE_PROJECT_REDUCER_H

#include "shared.h"
#include "../utils/word_parser.h"

void reduce_direct_index_phase(const char* filename, int rank){
    std::unordered_map<std::string, unsigned long long> word_map;

    //search in the directory with filename name
    auto full_path = concatenate_to_path(TEMP_FOLDER_DIRECT_IDX_PHASE_MAPPERS, filename);
    auto words_ptr = directory_utils::get_file_names(full_path);

    std::vector<std::string> word_vector;
    for (auto & temp_filename : *words_ptr){
        word_parser::split_string_by_separator(word_vector, temp_filename, '@');
        word_parser::update_word_dict(word_map, word_vector[1], std::stoull(word_vector[2]));
        word_vector.clear();
    }
    full_path = concatenate_to_path(TEMP_FOLDER_DIRECT_IDX_PHASE_REDUCERS, filename);
    printer::create_word_files_from_dictionary(full_path.c_str(), word_map, filename, rank);
    delete words_ptr;
}

void reduce_indirect_index_phase(const char* word){
    std::vector<PAIR_STRING_COUNT> word_list_with_file_pair;

    auto word_directory = concatenate_to_path(TEMP_FOLDER_INDIRECT_IDX_PHASE_MAPPERS, word);
    auto filenames_ptr = directory_utils::get_file_names(word_directory);

    std::vector<std::string> word_vector;
    for (auto & temp_filename : *filenames_ptr){
        word_parser::split_string_by_separator(word_vector, temp_filename, '@');
        int idx = 0;
        // find the position in the ordered vector
        for (; idx < word_list_with_file_pair.size(); ++idx){
            if (word_list_with_file_pair[idx].first >= word_vector[1])
                break;
        }
        word_list_with_file_pair.insert(word_list_with_file_pair.begin() + idx, std::make_pair(word_vector[1], std::stoull(word_vector[2])));
        word_vector.clear();
    }
    printer::create_word_files_from_filenames_pair(TEMP_FOLDER_INDIRECT_IDX_PHASE_REDUCERS, word_list_with_file_pair, word);

    delete filenames_ptr;
}

#endif //MPI_MAP_REDUCE_PROJECT_REDUCER_H
