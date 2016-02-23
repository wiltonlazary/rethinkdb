#include <iostream>

class data_t {
public:
    data_t(int num) {
    }
    ~data_t() {
        std::cout << "Destructor called.\n";
    }
};

struct data_struct_t {
    data_t some_data;
    data_t some_other_data;
};

int main() {
    {
        data_struct_t{1, 2};
    }
    return 0;
}
