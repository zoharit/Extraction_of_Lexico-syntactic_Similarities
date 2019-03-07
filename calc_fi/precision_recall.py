from sklearn.metrics import precision_recall_curve
import matplotlib.pyplot as plt
from sklearn.utils.fixes import signature
import numpy as np


def main():
	probs=[]
	with open("positive") as positive_file:
		lines=positive_file.readlines()
		lines=[x.split("\t") for x in lines]
		lines=[(1,p[2].strip()) for p in lines]
		probs=probs+lines
	with open("negative") as negative_file:
		lines=negative_file.readlines()
		lines=[x.split("\t") for x in lines]
		lines=[(0,p[2].strip()) for p in lines]
		probs=probs+lines
	y=np.array([p[0] for p in probs])
	y_score=np.array([float(p[1]) for p in probs])
	precision, recall, _ = precision_recall_curve(y, y_score)
# In matplotlib < 1.5, plt.fill_between does not have a 'step' argument
	step_kwargs = ({'step': 'post'} if 'step' in signature(plt.fill_between).parameters else {})
	plt.step(recall, precision, color='b', alpha=0.2,where='post')
	plt.fill_between(recall, precision, alpha=0.2, color='b', **step_kwargs)
	plt.xlabel('Recall')
	plt.ylabel('Precision')
	plt.ylim([0.8, 1.05])
	plt.xlim([0.0, 1.0])
	plt.show()
if __name__ == "__main__":
    main()
