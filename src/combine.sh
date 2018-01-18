for d in 201*/; do
	cat ${d}* > "${d%????}.txt"
done