import React, { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeHighlight from 'rehype-highlight';
import 'highlight.js/styles/github-dark.css';

export default function ChatInterface({ selectedDocIds = [], selectedText = '', onRelevanceUpdate, documents = [] }) {
    const [messages, setMessages] = useState([
        { role: 'agent', content: 'Hello! I am your research assistant. Ask me about academic papers, and I can help you find citations and generate research ideas.' }
    ]);
    const [input, setInput] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [modelPresets, setModelPresets] = useState({
        tuwien: { display_name: "TU Wien (GLM-4.6 + Mistral)" },
        gemini: { display_name: "Google Gemini 2.5 Flash" }
    });
    const [currentModel, setCurrentModel] = useState('gemini');
    const [isSwitchingModel, setIsSwitchingModel] = useState(false);
    const messagesEndRef = useRef(null);
    const sessionIdRef = useRef(Math.random().toString(36).substring(7));

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    };

    // Preprocess content to convert section headers to proper markdown headings
    const preprocessMarkdown = (content) => {
        // Convert "--- SECTION NAME ---" to a horizontal rule followed by heading
        // This creates a visual separator before each section
        return content.replace(/^---\s+([A-Z\s]+)\s+---$/gm, '\n---\n\n### $1');
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    // Fetch available models on component mount
    useEffect(() => {
        const fetchModels = async () => {
            try {
                console.log('[CHAT] Fetching available models from backend...');
                const response = await fetch('http://127.0.0.1:8000/api/models');
                if (response.ok) {
                    const data = await response.json();
                    console.log('[CHAT] Received model data:', data);
                    setModelPresets(data.presets);
                    setCurrentModel(data.current);
                } else {
                    console.error('[CHAT] Failed to fetch models, status:', response.status);
                }
            } catch (error) {
                console.error('[CHAT] Failed to fetch models:', error);
                console.log('[CHAT] Using default model presets');
            }
        };
        fetchModels();
    }, []);

    const handleModelSwitch = async (presetName) => {
        if (presetName === currentModel || isSwitchingModel) return;
        
        setIsSwitchingModel(true);
        try {
            const response = await fetch(`http://127.0.0.1:8000/api/models/switch/${presetName}`, {
                method: 'POST',
            });
            
            if (response.ok) {
                const data = await response.json();
                setCurrentModel(data.current);
                // Clear messages and reset session
                setMessages([
                    { role: 'agent', content: `Switched to ${modelPresets[presetName]?.display_name || presetName}. How can I help you?` }
                ]);
                sessionIdRef.current = Math.random().toString(36).substring(7);
            } else {
                console.error('Failed to switch model');
                setMessages(prev => [...prev, { 
                    role: 'agent', 
                    content: 'Failed to switch model. Please try again.' 
                }]);
            }
        } catch (error) {
            console.error('Error switching model:', error);
            setMessages(prev => [...prev, { 
                role: 'agent', 
                content: 'Error switching model. Please check if the backend is running.' 
            }]);
        } finally {
            setIsSwitchingModel(false);
        }
    };


    const handleSubmit = async (e) => {
        e.preventDefault();
        if (!input.trim() || isLoading) return;

        let userMessage = input.trim();
        let displayMessage = userMessage; // What the user sees in the chat bubble

        // Add document context if documents are selected
        if (selectedDocIds.length > 0) {
            userMessage = `[Path of requested documents: ${selectedDocIds.join(', ')}]\n${userMessage}`;
            // displayMessage stays as the user's original input
        }

        console.log(userMessage);
        console.log(selectedDocIds);

        setInput('');
        setMessages(prev => [...prev, {
            role: 'user',
            content: displayMessage,
            selectedDocs: selectedDocIds.length > 0 ? selectedDocIds : null
        }]);
        setIsLoading(true);

        try {
            const response = await fetch(`http://127.0.0.1:8000/api/chat/${sessionIdRef.current}?prompt=${encodeURIComponent(userMessage)}`, {
                method: 'POST',
                headers: {
                    'Accept': 'text/event-stream',
                },
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let agentResponse = '';
            let hasAddedMessage = false;
            let currentEvent = 'message'; // Track current SSE event type
            let eventDataLines = [];

            const processEvent = (eventType, dataLines) => {
                const data = dataLines.join('\n');
                if (!data) return;

                // Handle metadata events
                if (eventType === 'metadata') {
                    try {
                        const metadata = JSON.parse(data);
                        console.log('[CHAT] Received metadata event:', metadata);

                        if (metadata.recommendations && metadata.recommendations.scores) {
                            console.log('[CHAT] Triggering automatic document reordering');
                            if (onRelevanceUpdate) {
                                onRelevanceUpdate(metadata.recommendations.scores, true);
                            }
                        }
                    } catch (e) {
                        console.error('[CHAT] Failed to parse metadata:', e);
                    }
                    return;
                }

                // Handle regular message events
                if (data === '[DONE]') {
                    return;
                } else if (data.startsWith('[ERROR]')) {
                    console.error(data);
                    const errorText = "*An error occurred during processing.*";
                    agentResponse = errorText;
                    if (!hasAddedMessage) {
                        setMessages(prev => [...prev, { role: 'agent', content: agentResponse }]);
                        hasAddedMessage = true;
                    } else {
                        setMessages(prev => {
                            const newMessages = [...prev];
                            newMessages[newMessages.length - 1] = { role: 'agent', content: agentResponse };
                            return newMessages;
                        });
                    }
                } else {
                    // Replace the entire response (backend sends full content)
                    agentResponse = data;

                    // Add message on first content, update on subsequent chunks
                    if (!hasAddedMessage) {
                        setMessages(prev => [...prev, { role: 'agent', content: agentResponse }]);
                        hasAddedMessage = true;
                    } else {
                        setMessages(prev => {
                            const newMessages = [...prev];
                            newMessages[newMessages.length - 1] = { role: 'agent', content: agentResponse };
                            return newMessages;
                        });
                    }
                }
            };

            while (true) {
                const { value, done } = await reader.read();
                if (done) break;

                const chunk = decoder.decode(value, { stream: true });
                const lines = chunk.split('\n');

                for (const line of lines) {
                    if (line === '') {
                        if (eventDataLines.length > 0) {
                            processEvent(currentEvent, eventDataLines);
                        }
                        currentEvent = 'message';
                        eventDataLines = [];
                        continue;
                    }

                    if (line.startsWith('event: ')) {
                        currentEvent = line.slice(7).trim();
                        console.log(`[CHAT] Received SSE event type: ${currentEvent}`);
                    } else if (line.startsWith('data: ')) {
                        eventDataLines.push(line.slice(6));
                    }
                }
            }

        } catch (error) {
            console.error('Error fetching chat:', error);
            setMessages(prev => [...prev, { role: 'agent', content: "Sorry, I couldn't connect to the server. Please check if the backend is running." }]);
        } finally {
            setIsLoading(false);
        }
    };

    const handleShiftPlusEnter = (e) => {
        if (e.key === 'Enter') {
            if (e.shiftKey) {
                // Allow newline on Shift + Enter
                setInput((prev) => prev + '\n');
            } else {
                // Submit the form on Enter
                handleSubmit(e);
            }
        }
    };

    const handleInputChange = (e) => {
        setInput(e.target.value);
    };

    return (
        <div className="flex flex-col h-screen bg-gray-900 text-gray-100 font-sans">
            {/* Header */}
            <header className="bg-gradient-to-r from-gray-800 to-gray-900 border-b border-gray-700 shadow-lg">
                <div className="max-w-7xl mx-auto px-6 py-4">
                    <div className="flex items-center justify-between">
                        {/* Left: Title and Icon */}
                        <div className="flex items-center gap-3">
                            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 via-blue-600 to-purple-600 flex items-center justify-center shadow-lg">
                                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-6 h-6 text-white">
                                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 6.042A8.967 8.967 0 006 3.75c-1.052 0-2.062.18-3 .512v14.25A8.987 8.987 0 016 18c2.305 0 4.408.867 6 2.292m0-14.25a8.966 8.966 0 016-2.292c1.052 0 2.062.18 3 .512v14.25A8.987 8.987 0 0018 18a8.967 8.967 0 00-6 2.292m0-14.25v14.25" />
                                </svg>
                            </div>
                            <div>
                                <h1 className="text-lg font-bold text-white tracking-tight">Citation Discovery</h1>
                                <p className="text-xs text-gray-400">AI Research Assistant</p>
                            </div>
                        </div>
                        
                        {/* Right: Model Selector */}
                        <div className="flex items-center gap-3 bg-gray-700/50 rounded-lg px-4 py-2 backdrop-blur-sm border border-gray-600/50">
                            <div className="flex items-center gap-2">
                                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-4 h-4 text-gray-400">
                                    <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.456 2.456L21.75 6l-1.035.259a3.375 3.375 0 00-2.456 2.456zM16.894 20.567L16.5 21.75l-.394-1.183a2.25 2.25 0 00-1.423-1.423L13.5 18.75l1.183-.394a2.25 2.25 0 001.423-1.423l.394-1.183.394 1.183a2.25 2.25 0 001.423 1.423l1.183.394-1.183.394a2.25 2.25 0 00-1.423 1.423z" />
                                </svg>
                                <select
                                    id="model-selector"
                                    value={currentModel}
                                    onChange={(e) => handleModelSwitch(e.target.value)}
                                    disabled={isSwitchingModel || isLoading}
                                    className="bg-transparent text-white text-sm font-medium focus:outline-none cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed appearance-none pr-8"
                                    style={{
                                        backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 24 24' stroke='%239CA3AF'%3E%3Cpath stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M19 9l-7 7-7-7'%3E%3C/path%3E%3C/svg%3E")`,
                                        backgroundRepeat: 'no-repeat',
                                        backgroundPosition: 'right 0.5rem center',
                                        backgroundSize: '1.25rem'
                                    }}
                                >
                                    {Object.entries(modelPresets).map(([key, preset]) => (
                                        <option key={key} value={key} className="bg-gray-800">
                                            {preset.display_name}
                                        </option>
                                    ))}
                                </select>
                            </div>
                            {isSwitchingModel && (
                                <svg className="animate-spin h-4 w-4 text-blue-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                </svg>
                            )}
                        </div>
                    </div>
                </div>
            </header>

            {/* Chat Area */}
            <main className="flex-1 overflow-y-auto p-4 space-y-6 scroll-smooth">
                <div className="max-w-3xl mx-auto space-y-6">
                    {messages.map((msg, index) => {
                        // Helper function to get paper title from file path
                        const getPaperTitle = (filePath) => {
                            const doc = documents.find(d => d.file_path === filePath);
                            return doc ? doc.title : filePath;
                        };

                        return (
                            <div
                                key={index}
                                className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
                            >
                                <div
                                    className={`max-w-[80%] rounded-2xl px-5 py-3.5 shadow-sm ${msg.role === 'user'
                                        ? 'bg-blue-600 text-white rounded-br-none'
                                        : 'bg-gray-800 border border-gray-700 text-gray-200 rounded-bl-none'
                                        }`}
                                >
                                    {/* Show selected papers if any */}
                                    {msg.selectedDocs && msg.selectedDocs.length > 0 && (
                                        <div className="text-xs opacity-70 mb-2 pb-2 border-b border-white/20 break-all" style={{ overflowWrap: 'anywhere' }}>
                                            📄 {msg.selectedDocs.map(getPaperTitle).join(', ')}
                                        </div>
                                    )}
                                    <div className={`leading-relaxed text-[0.95rem] ${msg.role === 'user' ? 'whitespace-pre-wrap break-all' : 'markdown-content'}`} style={{ overflowWrap: 'anywhere' }}>
                                        {msg.role === 'user' ? (
                                            msg.content
                                        ) : (
                                            <ReactMarkdown
                                                remarkPlugins={[remarkGfm]}
                                                rehypePlugins={[rehypeHighlight]}
                                                components={{
                                                    // Style headings
                                                    h1: ({node, ...props}) => <h1 className="text-xl font-bold mt-4 mb-2" {...props} />,
                                                    h2: ({node, ...props}) => <h2 className="text-lg font-semibold mt-3 mb-2" {...props} />,
                                                    h3: ({node, ...props}) => <h3 className="text-base font-bold mt-3 mb-3 text-gray-100 uppercase tracking-wide" {...props} />,
                                                    // Style lists
                                                    ul: ({node, ...props}) => <ul className="list-disc list-inside my-2 space-y-1" {...props} />,
                                                    ol: ({node, ...props}) => <ol className="list-decimal list-inside my-2 space-y-1" {...props} />,
                                                    li: ({node, ...props}) => <li className="ml-2 break-all" style={{ overflowWrap: 'anywhere' }} {...props} />,
                                                    // Style code
                                                    code: ({node, inline, ...props}) => 
                                                        inline ? (
                                                            <code className="bg-gray-700 px-1.5 py-0.5 rounded text-sm break-all" style={{ overflowWrap: 'anywhere' }} {...props} />
                                                        ) : (
                                                            <code className="block bg-gray-700 p-3 rounded-lg my-2 overflow-x-auto text-sm" {...props} />
                                                        ),
                                                    pre: ({node, ...props}) => <pre className="bg-gray-700 rounded-lg my-2 overflow-x-auto" {...props} />,
                                                    // Style blockquotes
                                                    blockquote: ({node, ...props}) => <blockquote className="border-l-4 border-gray-600 pl-4 italic my-2" {...props} />,
                                                    // Style horizontal rules (thematic breaks) - used for section dividers
                                                    hr: ({node, ...props}) => {
                                                        // Full-width prominent divider for sections
                                                        return <hr className="border-t border-gray-600 my-6 -mx-5" style={{ borderTopWidth: '1px' }} {...props} />;
                                                    },
                                                    // Style links
                                                    a: ({node, ...props}) => <a className="text-blue-400 hover:text-blue-300 underline break-all" style={{ overflowWrap: 'anywhere' }} {...props} />,
                                                    // Style paragraphs
                                                    p: ({node, ...props}) => <p className="my-2" {...props} />,
                                                    // Style tables
                                                    table: ({node, ...props}) => <table className="border-collapse border border-gray-600 my-2" {...props} />,
                                                    thead: ({node, ...props}) => <thead className="bg-gray-700" {...props} />,
                                                    th: ({node, ...props}) => <th className="border border-gray-600 px-3 py-2 text-left" {...props} />,
                                                    td: ({node, ...props}) => <td className="border border-gray-600 px-3 py-2" {...props} />,
                                                }}
                                            >
                                                {preprocessMarkdown(msg.content)}
                                            </ReactMarkdown>
                                        )}
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                    {isLoading && (
                        <div className="flex justify-start">
                            <div className="bg-gray-800 border border-gray-700 rounded-2xl rounded-bl-none px-5 py-4 shadow-sm">
                                <div className="flex space-x-2">
                                    <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }}></div>
                                    <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }}></div>
                                    <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }}></div>
                                </div>
                            </div>
                        </div>
                    )}
                    <div ref={messagesEndRef} />
                </div>
            </main>

            {/* Input Area */}
            <footer className="p-4 bg-gray-900 border-t border-gray-800">
                <div className="max-w-3xl mx-auto">
                    {/* Selected Documents Indicator */}
                    {selectedDocIds.length > 0 && (
                        <div className="mb-3 flex items-center gap-2 text-sm text-gray-400 bg-gray-800 rounded-lg px-3 py-2 border border-gray-700">
                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="w-4 h-4 text-blue-400">
                                <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                            </svg>
                            <span>Asking about {selectedDocIds.length} selected document{selectedDocIds.length !== 1 ? 's' : ''}</span>
                        </div>
                    )}

                    <form onSubmit={handleSubmit} className="relative">
                        <textarea
                            value={input}
                            onChange={handleInputChange}
                            onKeyDown={handleShiftPlusEnter}
                            placeholder="Ask a research question..."
                            className="w-full bg-gray-800 text-white border border-gray-700 rounded-xl pl-5 pr-18 py-3.5 focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500 transition-all placeholder-gray-500 resize-none"
                            disabled={isLoading}
                            rows="1"
                            style={{
                                overflow: 'auto',
                            }}
                            onInput={(e) => {
                                e.target.style.height = 'auto';
                                e.target.style.height = Math.min(e.target.scrollHeight, 200) + 'px';

                                // Watch for programmatic clear (e.g. on submit) and reset height immediately
                                if (!e.target._resetHeightWatcher) {
                                    e.target._resetHeightWatcher = setInterval(() => {
                                        if (e.target.value === '') {
                                            e.target.style.height = 'auto';
                                            clearInterval(e.target._resetHeightWatcher);
                                            e.target._resetHeightWatcher = null;
                                        }
                                    }, 50);
                                }
                            }}
                        />
                        <button
                            type="submit"
                            disabled={!input.trim() || isLoading}
                            className="absolute right-2 top-2 p-2 bg-blue-600 hover:bg-blue-500 text-white rounded-lg disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        >
                            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
                                <path d="M3.478 2.405a.75.75 0 00-.926.94l2.432 7.905H13.5a.75.75 0 010 1.5H4.984l-2.432 7.905a.75.75 0 00.926.94 60.519 60.519 0 0018.445-8.986.75.75 0 000-1.218A60.517 60.517 0 003.478 2.405z" />
                            </svg>
                        </button>
                    </form>
                    <p className="text-center text-xs text-gray-500 mt-3">
                        AI can make mistakes. Please verify important information.
                    </p>
                </div>
            </footer>
        </div>
    );
}

