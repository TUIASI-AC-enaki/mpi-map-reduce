//
// Created by enaki on 12.01.2021.
//

#ifndef MPI_MAP_REDUCE_PROJECT_MASTER_H
#define MPI_MAP_REDUCE_PROJECT_MASTER_H

#include "../utils/directory_utils.h"
#include "shared.h"
#include "../utils/word_parser.h"
#include "../utils/printer.h"
#include <mpi.h>

void create_directories(){
    mkdir(TEMP_FOLDER,0777);
    mkdir(TEMP_FOLDER_DIRECT_IDX_PHASE,0777);
    mkdir(TEMP_FOLDER_DIRECT_IDX_PHASE_MAPPERS,0777);
    mkdir(TEMP_FOLDER_DIRECT_IDX_PHASE_REDUCERS,0777);
    mkdir(TEMP_FOLDER_INDIRECT_IDX_PHASE,0777);
    mkdir(TEMP_FOLDER_INDIRECT_IDX_PHASE_MAPPERS,0777);
    mkdir(TEMP_FOLDER_INDIRECT_IDX_PHASE_REDUCERS,0777);
    mkdir(OUTPUT_FOLDER,0777);
}

void create_directories_in_another_directory(const char* base_directory, std::vector<std::string> &directories){
    for (auto & directory : directories){
        std::stringstream output_file;
        output_file << base_directory << "/" << directory;
        mkdir(output_file.str().c_str(),0777);
    }
}

void master_index_phase(std::vector<std::string>* filenames_ptr, int phase){
    MPI_Status status;

    // CITIRE NUME FISIER SI TRIMITE CATRE WORKER CU COD DE MAP_DIRECT_INDEX_PHASE
    bool are_workers_active[WORKER_SIZE]{false};
    for (auto & filename : *filenames_ptr){
        // std::cout << "\t\t" << filename << std::endl;
        auto filename_size = strlen(filename.c_str())+1;
        int worker_destination = get_idle_worker(are_workers_active);
        if (worker_destination == -1){
            MPI_Recv(nullptr, 0, MPI_BYTE, MPI_ANY_SOURCE, FINISHED_WORK, MPI_COMM_WORLD, &status);
            worker_destination = status.MPI_SOURCE;
        }
        MPI_Send(filename.c_str(), filename_size, MPI_BYTE, worker_destination, phase, MPI_COMM_WORLD);
        are_workers_active[worker_destination] = true;
    }

    printf("[ MASTER ] - Etapa %s. Astept sa termine toti workerii\n", get_tag_name(phase));

    while(!all_workers_are_done(are_workers_active)){
        MPI_Recv(nullptr, 0, MPI_BYTE, MPI_ANY_SOURCE, FINISHED_WORK, MPI_COMM_WORLD, &status);
        log(MASTER, status.MPI_SOURCE, FINISHED_WORK);
        int worker_index = status.MPI_SOURCE;
        are_workers_active[worker_index] = false;
    }
    printf("[ MASTER ] - Etapa %s. Au terminat toti workerii\n\n", get_tag_name(phase));
}

void master_collect(std::string output_filename, std::string message = ""){
    std::stringstream output_file;
    output_file << OUTPUT_FOLDER << "/" << output_filename;
    std::ofstream outfile;

    auto output_filename_string = output_file.str();
    printer::create_truncated_file(output_filename_string);

    outfile.open(output_file.str(), std::ios_base::app); // append instead of overwrite
    outfile << "DICTIONARY WITH WORD AND LIST OF FILES\n";
    outfile << message << "\n";

    auto filenames_ptr = directory_utils::get_file_names(TEMP_FOLDER_INDIRECT_IDX_PHASE_REDUCERS);
    std::sort((*filenames_ptr).begin(), (*filenames_ptr).end());

    for (auto & temp_filename : *filenames_ptr) {
        std::vector<std::string> word_vector;
        word_parser::split_string_by_separator(word_vector, temp_filename, '@');
        // std::cout << temp_filename << std::endl;
        outfile << "\n-> " << word_vector[0] << "\n";
        for (auto index = 1; index < word_vector.size(); index+=2){
            outfile << "\t <" << word_vector[index] << ", " << word_vector[index+1] << ">\n";
        }
    }
}

#endif //MPI_MAP_REDUCE_PROJECT_MASTER_H
