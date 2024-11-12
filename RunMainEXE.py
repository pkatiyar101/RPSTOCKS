import subprocess

def run_program(choice):
    try:
        if choice == '1':
            subprocess.run(["RPSTOCKS.EXE"], check=True)
        
        elif choice == '2':
            subprocess.run(["PRIOPENrp.EXE"], check=True)
            
        elif choice == 'E':
            print("Exiting the program.")
            return False
        else:
            print("Invalid choice. Please select a valid option.")
    except KeyboardInterrupt:
        print("\nProgram interrupted. Returning to main menu...")
    except Exception as e:
        print(f"An error occurred: {e}")

    return True

def main():
    while True:
        print("------------------------------------------------------")
        print("*** Program designed by RPSTOCKS - KATIYAR - HLD ***")
        print("For Educational Purpose Only. Trust your own research.")
        print("------------------------------------------------------")
        print(" ")

        print("\nChoose which program to run:")
        print("------------------------------------------------------")
        print("1. Run ALL NSE PROGRAM (INTRA, FNO SWING, CASH)")
        print("2. Cash Stocks Pri Open Market Data")
        
        print(" ")        
        print("E. Exit")
        print("------------------------------------------------------")
        
        print(' 1, 2, (E)xit')
        choice = input("Enter your choice : ")
        if not run_program(choice):
            break

if __name__ == "__main__":
    main()
