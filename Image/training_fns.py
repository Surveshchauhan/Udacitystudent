import torch
import torchvision
import torch.nn as nn

def load_model(model_name):
    model=eval("torchvision.models.{}".format(model_name))
    model= model(pretrained=False)
    for param in model.parameters():
        param.requires_grad = False
        
        model.classifier[6]=nn.Linear(model.classifier[6].in_features,102)
        
    return model


def train_model(model, criterion, optimizer,train_dataloader, val_loader, device, num_epochs=25):
    
    
    best_acc=0
    for epoch in range(num_epochs):
        model.to(device)
        model.train()
        print('Epoch {}/{}'.format(epoch, num_epochs - 1))
        print('-' * 10)

        # Each epoch has a training and validation phase
            
            
        running_loss = 0.0
        running_corrects=0.0
        val_loss=0.0
        val_corrects=0.0
            
        for inputs, labels in train_dataloader:
            inputs = inputs.to(device)
            labels = labels.to(device)
                
                # zero the parameter gradients
            optimizer.zero_grad()
                
            outputs=model(inputs)
            _, preds=torch.max(outputs,1)
            loss=criterion(outputs, labels)
            loss.backward()
            optimizer.step()
                
                # statistics
            running_loss += loss.item() * inputs.size(0)
            running_corrects += torch.sum(preds ==labels.data)
            
                
        epoch_loss = running_loss / len(train_dataloader)
        epoch_acc= running_corrects / len(train_dataloader)

            
        print('Epoch LOSS: {}'.format(epoch_loss))
        print('Epoch ACC: {}'.format(epoch_acc))

            
        model.eval()
        for inputs, labels in val_loader:
            inputs = inputs.to(device)
            labels = labels.to(device)
                
            optimizer.zero_grad()
                
            outputs=model(inputs)
            loss=criterion(outputs, labels)
            val_loss += loss.item() * inputs.size(0)
            #val_corrects += torch.sum(preds==labels.data)
            
             # Calculate accuracy
            ps = torch.exp(outputs)
            top_p, top_class = ps.topk(1, dim=1)
            equals = top_class == labels.view(*top_class.shape)
            val_corrects += torch.mean(equals.type(torch.FloatTensor)).item()

        total_val_loss = val_loss / len(val_loader)
        total_val_acc = val_corrects / len(val_loader)
        
        print('Validation LOSS: {}'.format(total_val_loss))
        print('Validation ACC: {}'.format(total_val_acc))

        if  total_val_acc>best_acc:
            best_acc=total_val_acc
            #best_model_wts=copy.deepcopy(model.state_dict())
            torch.save(model, "./best_model.pth")
            
    return model
