//
// Created by enaki on 12.01.2021.
//

#ifndef MPI_MAP_REDUCE_PROJECT_MAPPER_H
#define MPI_MAP_REDUCE_PROJECT_MAPPER_H

#include "shared.h"
#include "../utils/file_reader.h"
#include "../utils/word_parser.h"
#include "../utils/printer.h"

void mapper_direct_index_phase(const char* directory_name, const char* filename, int rank){
    // ***READ LINES FROM FILENAME RECEIVED***;
    auto lines_in_file_ptr = file_reader::get_lines(directory_name, filename);

    // ***SPLIT LINES INTO WORDS USING A DICTIONARY FOR COUNTING***;
    std::vector<std::string> word_vector;
    std::stringstream filename_builder;

    for (auto &line: *lines_in_file_ptr){
        word_parser::split_words(word_vector, line);
        printer::create_word_files_from_vector(TEMP_FOLDER_DIRECT_IDX_PHASE_MAPPERS, word_vector, filename, rank);
        word_vector.clear();
    }
    delete lines_in_file_ptr;
}

void mapper_indirect_index_phase(std::vector<std::string>* filenames_ptr, const char* filename, int rank){
    // CONVERT FROM DIRECT INDEX TO INDIRECT

    std::vector<std::string> word_vector;
    for (auto & temp_filename : *filenames_ptr){
        word_parser::split_string_by_separator(word_vector, temp_filename, '@');
        auto compare_result = strcmp(filename, word_vector[0].c_str());
        if (compare_result == 0){
            auto word = word_vector[1];
            auto count = word_vector[2];
            auto timestamp = word_vector[3];
            printer::create_word_file_indirect_index(TEMP_FOLDER_INDIRECT_IDX_PHASE_MAPPERS, filename, word.c_str(), std::stoull(count), std::stoull(timestamp), rank);
            printer::create_word_file_if_not_exists(TEMP_FOLDER_INDIRECT_IDX_PHASE_WORDS, word.c_str());
        } else if (compare_result < 0){
            break;
        }
        word_vector.clear();
    }
}

#endif //MPI_MAP_REDUCE_PROJECT_MAPPER_H
