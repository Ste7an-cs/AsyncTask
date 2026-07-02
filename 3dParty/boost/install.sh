#! /bin/bash
cd `dirname $0`
7z x ./boost_1_89_0.7z
cd boost_1_89_0
./bootstrap.sh --with-libraries=all
sudo ./b2 -a install cxxflags="-std=c++17" link=static variant=release threading=multi debug-symbols=on

exit 0
