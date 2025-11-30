import React, { useState } from 'react';
import ChatInterface from './components/ChatInterface';
import DocumentExplorer from './components/DocumentExplorer';

function App() {
  const [selectedDocIds, setSelectedDocIds] = useState([]);
  const [selectedText, setSelectedText] = useState('');

  return (
    <div className="flex h-screen w-full bg-gray-900">
      {/* Document Explorer Sidebar */}
      <DocumentExplorer
        onDocumentSelect={setSelectedDocIds}
        selectedDocIds={selectedDocIds}
      />

      {/* Chat Interface */}
      <div className="flex-1">
        <ChatInterface
          selectedDocIds={selectedDocIds}
          selectedText={selectedText}
        />
      </div>
    </div>
  );
}

export default App;
