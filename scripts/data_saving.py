import os


if __name__ == "__main__":
	os.chdir("/home/pavol/Plocha/Agel/scripts")
	with open("saving.txt", "w") as file:
		file.write("Saved!")
