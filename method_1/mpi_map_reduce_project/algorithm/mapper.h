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
        auto full_path = concatenate_to_path(TEMP_FOLDER_DIRECT_IDX_PHASE_MAPPERS, filename);
        word_parser::split_words(word_vector, line);
        printer::create_word_files_from_vector(full_path.c_str(), word_vector, filename, rank);
        word_vector.clear();
        filename_builder.str("");
    }
    free(lines_in_file_ptr);
}

void mapper_indirect_index_phase(const char* filename, int rank){
    // CONVERT FROM DIRECT INDEX TO INDIRECT

    auto full_path = concatenate_to_path(TEMP_FOLDER_DIRECT_IDX_PHASE_REDUCERS, filename);
    auto filenames_ptr = directory_utils::get_file_names(full_path);

    std::vector<std::string> word_vector;
    for (auto & temp_filename : *filenames_ptr){
        word_parser::split_string_by_separator(word_vector, temp_filename, '@');
        auto word = word_vector[1];
        auto count = word_vector[2];
        auto word_directory = concatenate_to_path(TEMP_FOLDER_INDIRECT_IDX_PHASE_MAPPERS, word.c_str());
        mkdir(word_directory.c_str(), 0777);

        printer::create_word_file_indirect_index(word_directory.c_str(), filename, word.c_str(), std::stoull(count), rank);
        word_vector.clear();
    }
    free(filenames_ptr);
}

#endif //MPI_MAP_REDUCE_PROJECT_MAPPER_H
