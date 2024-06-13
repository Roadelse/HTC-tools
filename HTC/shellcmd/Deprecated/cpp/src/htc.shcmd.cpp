#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <cassert>
#include <iterator>
#include <algorithm>
#include <stdexcept>
#include <filesystem>
#include <regex>
#include <variant>

#ifdef USE_MPI
#include <mpi.h>
#endif

#include <toml/toml.hpp>

using jdvalue = std::variant<int, float, double, std::string>;

bool with_mpi = false;
int rank = 0;
int size = 1;

class Jobdef
{
public:
    Jobdef(const std::string &_jdfile)
    {
        if (!std::filesystem::exists(_jdfile))
        {
            printf("jdfile doesn't exist\n");
        }
        if (!std::regex_search(_jdfile, std::regex("\\.toml$")))
        {
            printf("jdfile is not a toml file");
        }

        jdfile = _jdfile;
        // if (!with_mpi || rank == 0)
        // {
        resolveParams();
        //     buildCommands();
        // }
        // if (with_mpi && size > 1)
        // {
        //     MPI_Bcast(&commands, sizeof(commands), MPI_BYTE, 0, comm);
        // }
    }

    //     void run()
    //     {
    //         if (with_mpi)
    //         {
    //             for (int i = rank; i < commands.size(); i += size)
    //             {
    //                 run_cmdset(commands[i]);
    //             }
    //         }
    //         else
    //         {
    //             for (const auto &cmdset : commands)
    //             {
    //                 run_cmdset(cmdset);
    //             }
    //         }
    //     }

private:
    std::string jdfile;
    std::vector<std::vector<std::string>> commands;
    // std::map<std::string, toml::value> defdict;
    std::vector<std::map<std::string, jdvalue>> params;

    //     void run_cmdset(const std::vector<std::string> &cmdset)
    //     {
    //         for (const auto &cmd : cmdset)
    //         {
    //             int rstat = system(cmd.c_str());
    //             if (rstat != 0)
    //             {
    //                 throw std::runtime_error("Error in os run: " + cmd);
    //             }
    //         }
    //     }

    void resolveParams()
    {
        std::ifstream file(jdfile);
        auto jdef = toml::parse(file);

        std::map<std::string, jdvalue> params_solid;
        std::vector<std::map<std::string, jdvalue>> params_product;
        std::vector<std::map<std::string, jdvalue>> params_zip;


        if (auto tableP = jdef["params"].as_table())
        {
            for (const auto &[key, value] : *tableP)
            {
                if (value.is_table())
                    continue;
                std::string keystr{key};
                if (value.is_integer())
                {
                    std::cout << "find solid param. Key = " << keystr << ", Value = " << value.value_or(0) << "\n";
                    params_solid[keystr] = value.value_or(0);
                }
                else if (value.is_floating_point())
                {
                    std::cout << "find solid param. Key = " << keystr << ", Value = " << value.value_or(0.) << "\n";
                    params_solid[keystr] = value.value_or(0.);
                }
                else if (value.is_string())
                {
                    std::cout << "find solid param. Key = " << keystr << ", Value = " << value.value_or("") << "\n";
                    params_solid[keystr] = value.value_or("");
                }
                else
                    throw std::runtime_error("Unknown type for value in toml");
            }

            if (auto table_pp = (*tableP)["product"].as_table()){
                
            }

            if (auto table_pz = (*tableP)["zip"].as_table()){

            }
        } else {
            throw std::runtime_error("Cannot recognize jdef[\"params\"] as a toml::table");
        }

        // std::cout << jdef << "\n";
        // std::string s1 = jdef["commands"][0].value_or("");
        // std::cout << "s1: " << s1 << std::endl;
        // std::map<std::string, toml::value> params_solid;
        // for (const auto &[k, v] : toml::find<std::map<std::string, toml::value>>(defdict, "params"))
        // {
        //     if (!v.is<toml::table>())
        //     {
        //         params_solid[k] = v;
        //     }
        // }
        // auto params_product = borrowed_product_withkey(toml::find<std::map<std::string, toml::value>>(defdict, "params.product"));
        // auto params_zip = borrowed_zip_withkey(toml::find<std::map<std::string, toml::value>>(defdict, "params.zip"));
        // for (const auto &a : params_solid)
        // {
        //     for (const auto &b : params_product)
        //     {
        //         for (const auto &c : params_zip)
        //         {
        //             params.push_back({a, b, c});
        //         }
        //     }
        // }
    }

    //     void buildCommands()
    //     {
    //         for (const auto &p : params)
    //         {
    //             std::vector<std::string> cmds;
    //             for (const auto &cmd : toml::find<std::vector<std::string>>(defdict, "commands"))
    //             {
    //                 cmds.push_back(update_cmd(cmd, p));
    //             }
    //             commands.push_back(cmds);
    //         }
    //     }

    //     std::string update_cmd(const std::string &cmd, const std::map<std::string, std::string> &p)
    //     {
    //         std::string updated_cmd = cmd;
    //         for (const auto &[k, v] : p)
    //         {
    //             std::string placeholder = "<" + k + ">";
    //             size_t pos = updated_cmd.find(placeholder);
    //             while (pos != std::string::npos)
    //             {
    //                 updated_cmd.replace(pos, placeholder.length(), v);
    //                 pos = updated_cmd.find(placeholder, pos + v.length());
    //             }
    //         }
    //         return updated_cmd;
    //     }

    // std::vector<std::map<std::string, std::string>> borrowed_zip_withkey(const std::map<std::string, toml::value> &D)
    // {
    //     size_t length = -1;
    //     for (const auto &[k, v] : D)
    //     {
    //         assert(v.is<toml::array>() && "Different length for list in zip_withkey values");
    //         if (length == -1)
    //         {
    //             length = v.as_array()->size();
    //         }
    //         assert(length == v.as_array()->size() && "Different length for list in zip_withkey values");
    //     }

    //     std::vector<std::map<std::string, std::string>> rst;
    //     std::vector<std::string> keys;
    //     for (const auto &[k, v] : D)
    //     {
    //         keys.push_back(k);
    //     }
    //     for (size_t i = 0; i < length; ++i)
    //     {
    //         std::map<std::string, std::string> ele;
    //         for (const auto &[k, v] : D)
    //         {
    //             ele[k] = v.as_array()->at(i).as_string();
    //         }
    //         rst.push_back(ele);
    //     }
    //     return rst;
    // }

    // std::vector<std::map<std::string, std::string>> borrowed_product_withkey(const std::map<std::string, toml::value> &D)
    // {
    //     std::vector<std::map<std::string, std::string>> rst;
    //     std::vector<std::string> keys;
    //     for (const auto &[k, v] : D)
    //     {
    //         keys.push_back(k);
    //     }
    //     std::vector<std::vector<std::string>> values;
    //     for (const auto &[k, v] : D)
    //     {
    //         std::vector<std::string> val;
    //         for (const auto &ele : *v.as_array())
    //         {
    //             val.push_back(ele.as_string());
    //         }
    //         values.push_back(val);
    //     }
    //     for (const auto &ele : cartesian_product(values))
    //     {
    //         std::map<std::string, std::string> map_ele;
    //         for (size_t i = 0; i < keys.size(); ++i)
    //         {
    //             map_ele[keys[i]] = ele[i];
    //         }
    //         rst.push_back(map_ele);
    //     }
    //     return rst;
    // }
};

// bool file_exists(const std::string &name)
// {
//     struct stat buffer;
//     return (stat(name.c_str(), &buffer) == 0);
// }

// void utest()
// {
//     int rank = 0;
//     bool with_mpi = false;

// #ifdef WITH_MPI
//     with_mpi = true;
//     MPI_Comm_rank(MPI_COMM_WORLD, &rank);
// #endif

//     if (!with_mpi || rank == 0)
//     {
//         std::ofstream f("__utest.toml");
//         f << R"(
// commands = ["echo <ct1><ct2><ct3><ct4><ct5>"]

// [params]
// ct1 = 1

// [params.product]
// ct4 = ["x"]
// ct5 = ["y", "z"]

// [params.zip]
// ct2 = ["a", "b"]
// ct3 = ["c", "d"]
// )";
//         f.close();
//     }

//     if (with_mpi)
//     {
//         try
//         {
//             Jobdef jd("__utest.toml");
//             jd.run();
//         }
//         catch (const std::exception &e)
//         {
//             std::cerr << "\033[32mFailure in unittest for htc.shcmd.cpp\033[0m" << std::endl;
//             // std::cerr << e.what() << std::endl;
//             MPI_Abort(MPI_COMM_WORLD, 1);
//         }
//         if (rank == 0)
//         {
//             remove("__utest.toml");
//         }
//     }
//     else
//     {
//         try
//         {
//             Jobdef jd("__utest.toml");
//             jd.run();
//         }
//         catch (...)
//         {
//             throw;
//         }
//         finally
//         {
//             remove("__utest.toml");
//         }
//     }
// }

int main(int argc, char *argv[])
{
    bool with_mpi = false;
    int rank = 0;
#ifdef USE_MPI
    printf("mpi detected\n");
#endif
    printf("hello\n");

    Jobdef jdo("test.toml");

    // #ifdef WITH_MPI
    //     MPI_Init(&argc, &argv);
    //     with_mpi = true;
    //     MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // #endif

    //     if (argc > 1)
    //     {
    //         std::string arg = argv[1];
    //         if (arg == "utest")
    //         {
    //             utest();
    // #ifdef WITH_MPI
    //             MPI_Finalize();
    // #endif
    //             return 0;
    //         }
    //         else if (arg == "help")
    //         {
    //             if (!with_mpi || rank == 0)
    //             {
    //                 std::cout << R"(
    // [Usage]
    //     ./htc.shcmd [infile]

    //     where, [infile] will be jobdef.toml if omitted

    // [infile grammar]
    //     toml file, with format like:
    // >>>>>>>>>>
    // commands = ["echo <ct1><ct2><ct3><ct4><ct5>"]

    // [params]
    // ct1 = 1

    // [params.product]
    // ct4 = ["x"]
    // ct5 = ["y", "z"]

    // [params.zip]
    // ct2 = ["a", "b"]
    // ct3 = ["c", "d"]
    // >>>>>>>>>>)";
    //             }
    // #ifdef WITH_MPI
    //             MPI_Finalize();
    // #endif
    //             return 0;
    //         }
    //         std::string jobDefFile = argv[1];
    //         if (!file_exists(jobDefFile))
    //         {
    //             std::cerr << "Cannot find jobDefFile: " << jobDefFile << std::endl;
    // #ifdef WITH_MPI
    //             MPI_Finalize();
    // #endif
    //             return 1;
    //         }
    //         Jobdef jd(jobDefFile);
    //         jd.run();
    //     }
    //     else
    //     {
    //         std::string jobDefFile = "jobdef.toml";
    //         if (!file_exists(jobDefFile))
    //         {
    //             std::cerr << "Cannot find jobDefFile: " << jobDefFile << std::endl;
    // #ifdef WITH_MPI
    //             MPI_Finalize();
    // #endif
    //             return 1;
    //         }
    //         Jobdef jd(jobDefFile);
    //         jd.run();
    //     }

    // #ifdef WITH_MPI
    //     MPI_Finalize();
    // #endif

    return 0;
}
