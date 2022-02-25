import torch
import argparse
from utils.training_fns import train_model, load_model
from utils.load_data import load_data
from torch.optim import Adam
import torch.nn as nn

parser =argparse.ArgumentParser(description= 'Train a NN for Image classification.')
parser.add_argument('train_dir')
parser.add_argument('--device', default="gpu")
parser.add_argument('--architecture', default="vgg16")
parser.add_argument('--epochs', default=1)


args = parser.parse_args()
train_dir = args.train_dir
model_name=args.architecture
epochs=args.epochs
device=args.device
train_load, val_load, test_load = load_data(train_dir)

model=load_model(model_name)
optimizer=Adam(model.classifier[6].parameters(), lr=0.0001)
loss= nn.CrossEntropyLoss()  

if device=="gpu":
    device=torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
else:
    device=="cpu"

train_model(model, loss, optimizer,train_load, val_load, device=device, num_epochs=epochs)



                               